//! Samples for <https://stratamesh.dev/guides/graphs/retries-and-backoff/>.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult, HumanSignal};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, Jitter, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{RunEventChannel, RunEventKind};
use datalium_strata::schema::CheckpointStatus;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

const NEW_CARD: &str = "tok_visa_4242";
const EXPIRED_CARD: &str = "tok_expired_0119";

// docs:begin guides/graphs/retries-and-backoff/state
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderState {
    order_id: String,
    card_token: String,
    status: String,
    receipt_id: Option<String>,
    confirmed: bool,
    review_note: Option<String>,
}
// docs:end guides/graphs/retries-and-backoff/state

fn order(order_id: &str, card_token: &str) -> OrderState {
    OrderState {
        order_id: order_id.to_string(),
        card_token: card_token.to_string(),
        status: "new".to_string(),
        receipt_id: None,
        confirmed: false,
        review_note: None,
    }
}

/// The stand-in for your own client's error type.
struct GatewayError {
    status: u16,
    retry_after: Option<Duration>,
    message: String,
}

impl GatewayError {
    fn new(status: u16, message: &str, retry_after: Option<Duration>) -> Self {
        Self {
            status,
            retry_after,
            message: message.to_string(),
        }
    }
}

/// How the stand-in payment gateway misbehaves. Whatever the mode, it
/// declines an expired card.
#[derive(Clone, Copy)]
enum Outage {
    None,
    /// A 503 for the first two calls, then it works.
    TwoUnavailable,
    /// Every call is a 429 that asks the caller to wait 30 seconds.
    RateLimited,
    /// Every call hangs until it is cancelled.
    Hangs,
}

struct Gateway {
    outage: Outage,
    calls: AtomicU32,
    idempotency_keys: Mutex<Vec<String>>,
}

impl Gateway {
    fn new(outage: Outage) -> Arc<Self> {
        Arc::new(Self {
            outage,
            calls: AtomicU32::new(0),
            idempotency_keys: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    async fn charge(
        &self,
        order: &OrderState,
        idempotency_key: &str,
    ) -> Result<String, GatewayError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.idempotency_keys
            .lock()
            .unwrap()
            .push(idempotency_key.to_string());
        match self.outage {
            Outage::TwoUnavailable if call <= 2 => {
                return Err(GatewayError::new(503, "service unavailable", None));
            }
            Outage::RateLimited => {
                let after = Some(Duration::from_secs(30));
                return Err(GatewayError::new(429, "too many requests", after));
            }
            Outage::Hangs => std::future::pending::<()>().await,
            _ => {}
        }
        if order.card_token.starts_with("tok_expired") {
            return Err(GatewayError::new(402, "card declined: expired", None));
        }
        Ok(format!("RCPT-{}-{call}", order.order_id))
    }
}

/// `ChargePayment` -> `SendConfirmation`, and `QueueForManualReview` when the
/// policy falls back to it: the policy is its only way in.
fn order_nodes(gateway: Arc<Gateway>, with_fallback: bool) -> StateGraph<OrderState> {
    let graph = StateGraph::<OrderState>::new()
        // docs:begin guides/graphs/retries-and-backoff/fallible-node
        .add_async_fallible_node(
            "ChargePayment",
            move |order: OrderState, ctx: NodeContext| {
                let gateway = gateway.clone();
                async move {
                    match gateway.charge(&order, ctx.idempotency_key()).await {
                        Ok(receipt) => Ok(OrderState {
                            receipt_id: Some(receipt),
                            status: "charged".into(),
                            ..order
                        }),
                        // Worth retrying. Pass the provider's Retry-After on, if it sent one.
                        Err(e) if matches!(e.status, 429 | 503) => {
                            let message = format!("gateway unavailable ({})", e.status);
                            Err(match e.retry_after {
                                Some(after) => NodeError::transient_after(message, after),
                                None => NodeError::Transient(message),
                            })
                        }
                        // Retrying won't help.
                        Err(e) => Err(NodeError::Fatal(e.message)),
                    }
                }
            },
        )
        // docs:end guides/graphs/retries-and-backoff/fallible-node
        .add_node("SendConfirmation", |order: OrderState| OrderState {
            confirmed: true,
            ..order
        })
        // docs:begin guides/graphs/retries-and-backoff/timeout
        .set_timeout("ChargePayment", Duration::from_secs(1))
        // docs:end guides/graphs/retries-and-backoff/timeout
        .add_edge("ChargePayment", "SendConfirmation");

    if with_fallback {
        graph
            // docs:begin guides/graphs/retries-and-backoff/fallback
            .add_async_node(
                "QueueForManualReview",
                |order: OrderState, ctx: NodeContext| async move {
                    let failure = ctx
                        .last_failure()
                        .expect("a fallback node has a last failure");
                    let kind = if failure.error.is_transient() {
                        "Transient"
                    } else {
                        "Fatal"
                    };
                    let mut note = format!(
                        "{} failed ({kind}) after {} attempt(s): {}",
                        failure.node,
                        failure.attempts,
                        failure.error.message()
                    );
                    if let Some(wait) = failure.error.retry_after() {
                        note += &format!("; asked to wait {} s", wait.as_secs());
                    }
                    if failure.timed_out {
                        note += "; timed out";
                    }
                    OrderState {
                        status: "manual_review".into(),
                        review_note: Some(note),
                        ..order
                    }
                },
            )
        // docs:end guides/graphs/retries-and-backoff/fallback
    } else {
        graph
    }
}

fn order_graph(gateway: Arc<Gateway>, with_fallback: bool) -> CompiledGraph<OrderState> {
    // docs:begin guides/graphs/retries-and-backoff/policy
    let policy = RecoveryPolicy::retry(2)
        .with_backoff(
            Duration::from_millis(200),
            Duration::from_secs(2),
            Jitter::None,
        )
        .with_fallback("QueueForManualReview");

    // docs:hide ...
    let mut policy = policy;
    if !with_fallback {
        policy.fallback = None;
    }
    order_nodes(gateway, with_fallback)
        // docs:show
        .set_recovery_policy("ChargePayment", policy)
        // docs:end guides/graphs/retries-and-backoff/policy
        .compile()
        .expect("the order graph is structurally valid")
}

/// A retry the engine scheduled: the attempt that failed, the wait, and why.
type Retry = (u32, Duration, String);

/// Runs `order` on its own thread, and collects the retries from its run events.
async fn run_watched(
    graph: &CompiledGraph<OrderState>,
    order: OrderState,
    store: Arc<dyn CheckpointStore>,
) -> (GraphExecutionResult<OrderState>, Vec<Retry>) {
    let thread_id = order.order_id.clone();
    let mut retries = Vec::new();

    // docs:begin guides/graphs/retries-and-backoff/watch
    let (observer, mut events) = RunEventChannel::new();
    let result = graph
        .run_with(
            &thread_id,
            order,
            store,
            RunConfig::new().with_observer(observer),
        )
        .await;
    while let Some(event) = events.recv().await {
        if let RunEventKind::RetryScheduled { delay, error, .. } = event.kind {
            println!(
                "  retry scheduled after attempt {}: wait {} ms ({error})",
                event.attempt.unwrap_or_default(),
                delay.as_millis()
            );
            // docs:hide
            retries.push((event.attempt.unwrap_or_default(), delay, error));
            // docs:show
        }
    }
    // docs:end guides/graphs/retries-and-backoff/watch

    (result, retries)
}

fn expect_completed(result: GraphExecutionResult<OrderState>) -> OrderState {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("the run failed: {err:?}"),
        _ => panic!("the run didn't complete"),
    }
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

// The paused clock runs the waits at once: the delays come from the engine's events.
#[tokio::test(start_paused = true)]
async fn two_unavailable_then_success() {
    let gateway = Gateway::new(Outage::TwoUnavailable);
    let graph = order_graph(gateway.clone(), true);
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    let (result, retries) = run_watched(&graph, order("ord-1001", NEW_CARD), store).await;

    let error = "gateway unavailable (503)".to_string();
    assert_eq!(retries, [(1, ms(200), error.clone()), (2, ms(400), error)]);
    let state = expect_completed(result);
    assert_eq!(state.status, "charged");
    assert_eq!(state.receipt_id.as_deref(), Some("RCPT-ord-1001-3"));
    assert!(state.confirmed);
    // Every retry passes the service the same idempotency key.
    let keys = gateway.idempotency_keys.lock().unwrap().clone();
    assert_eq!(keys.len(), 3);
    assert!(keys.iter().all(|key| *key == keys[0]));
}

#[tokio::test(start_paused = true)]
async fn rate_limited_then_fall_back() {
    let graph = order_graph(Gateway::new(Outage::RateLimited), true);
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    let (result, retries) = run_watched(&graph, order("ord-1002", NEW_CARD), store).await;

    // The 30-second hint is capped at the backoff's 2-second maximum.
    let error = "gateway unavailable (429)".to_string();
    let wait = Duration::from_secs(2);
    assert_eq!(retries, [(1, wait, error.clone()), (2, wait, error)]);
    let state = expect_completed(result);
    assert_eq!(state.status, "manual_review");
    assert_eq!(state.receipt_id, None);
    assert!(!state.confirmed);
    assert_eq!(
        state.review_note.as_deref(),
        Some(
            "ChargePayment failed (Transient) after 3 attempt(s): gateway unavailable (429); asked to wait 30 s"
        )
    );
}

#[tokio::test(start_paused = true)]
async fn hung_attempts_time_out() {
    let gateway = Gateway::new(Outage::Hangs);
    let graph = order_graph(gateway.clone(), true);
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    let (result, retries) = run_watched(&graph, order("ord-1003", NEW_CARD), store).await;

    assert_eq!(gateway.calls(), 3);
    let error = "node 'ChargePayment' timed out after 1000 ms".to_string();
    assert_eq!(retries, [(1, ms(200), error.clone()), (2, ms(400), error)]);
    let state = expect_completed(result);
    assert_eq!(state.status, "manual_review");
    assert_eq!(
        state.review_note.as_deref(),
        Some(
            "ChargePayment failed (Transient) after 3 attempt(s): node 'ChargePayment' timed out after 1000 ms; timed out"
        )
    );
}

#[tokio::test(start_paused = true)]
async fn resume_a_failed_thread() {
    // A declined card, with no fallback: one attempt, since the failure is fatal.
    let store_2: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let declined = order_graph(Gateway::new(Outage::None), false)
        .run(
            "order-1003",
            order("ord-1004", EXPIRED_CARD),
            store_2.clone(),
        )
        .await;
    match declined {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            assert_eq!(node, "ChargePayment");
            assert_eq!(attempts, 1);
            assert_eq!(message, "card declined: expired");
        }
        _ => panic!("expected NodeFailed"),
    }
    let checkpoint = store_2
        .load("order-1003")
        .await
        .unwrap()
        .expect("a failed checkpoint");
    assert_eq!(checkpoint.status, CheckpointStatus::Failed);
    assert_eq!(checkpoint.next_node.as_deref(), Some("ChargePayment"));
    let metadata = checkpoint.metadata.expect("metadata");
    assert_eq!(metadata["attempts"], "1");
    assert_eq!(metadata["error"], "card declined: expired");
    assert_eq!(metadata["error_kind"], "fatal");

    let resume_gateway = Gateway::new(Outage::None);
    let resume_graph = order_graph(resume_gateway.clone(), false);

    // docs:begin guides/graphs/retries-and-backoff/resume
    let fix = HumanSignal::approve("support@shop.example").with_override("card_token", NEW_CARD);
    let resumed = expect_completed(
        resume_graph
            .resume("order-1003", fix.clone(), store_2.clone())
            .await,
    );
    // docs:end guides/graphs/retries-and-backoff/resume

    assert_eq!(resume_gateway.calls(), 1);
    assert_eq!(resumed.status, "charged");
    assert!(resumed.confirmed);
    assert_eq!(resumed.card_token, NEW_CARD);

    // A failed checkpoint can be resumed once.
    let again = resume_graph.resume("order-1003", fix, store_2).await;
    assert!(matches!(again, GraphExecutionResult::Failed(_)));
    assert_eq!(resume_gateway.calls(), 1);
}
