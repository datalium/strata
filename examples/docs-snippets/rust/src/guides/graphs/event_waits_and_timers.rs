//! Samples for <https://stratamesh.dev/guides/graphs/event-waits-and-timers/>.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointQuery, EventSignal, GraphExecutionResult, HumanSignal, LevelSignals, ThreadQuery,
    WakeReason,
};
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::schema::WaitKind;
use datalium_strata::wait::WaitOptions;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    paid: bool,
    payment_ref: Option<String>,
    shipped: bool,
}

#[allow(
    clippy::let_and_return,
    reason = "the sample binds `orders`, the name the page uses"
)]
fn order_graph() -> Arc<CompiledGraph<Order>> {
    // docs:begin guides/graphs/event-waits-and-timers/graph
    const HOUR: Duration = Duration::from_secs(3600);

    let orders = Arc::new(
        StateGraph::<Order>::new()
            .add_node("Place", |o: Order| o)
            .add_wait_for_event_with(
                "AwaitPayment",
                "payment.received",
                WaitOptions::new().with_timeout(24 * HOUR),
            )
            .add_node("Pack", |o: Order| o)
            .add_timer("AwaitPickup", 4 * HOUR)
            .add_node("Ship", |o: Order| Order { shipped: true, ..o })
            .add_edge("Place", "AwaitPayment")
            .add_edge("AwaitPayment", "Pack")
            .add_edge("Pack", "AwaitPickup")
            .add_edge("AwaitPickup", "Ship")
            .compile()
            .expect("the order graph is valid"),
    );
    // docs:end guides/graphs/event-waits-and-timers/graph
    orders
}

/// The message of a refusal.
fn refusal(result: &GraphExecutionResult<Order>) -> &str {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        GraphExecutionResult::Failed(other) => panic!("expected a refusal, got {other:?}"),
        other => panic!("expected a refusal, got {}", outcome(other)),
    }
}

#[tokio::test]
async fn wait_signal_and_wake() {
    let orders = order_graph();
    let mut waiting = Vec::new();

    // docs:begin guides/graphs/event-waits-and-timers/run
    let store = Arc::new(InMemoryCheckpointStore::new());
    let start = Utc::now();
    for id in ["order-1", "order-2"] {
        let order = Order {
            order_id: id.into(),
            paid: false,
            payment_ref: None,
            shipped: false,
        };
        if let GraphExecutionResult::Interrupted { wait, .. } =
            orders.run(id, order, store.clone()).await
        {
            let wait = wait.unwrap();
            println!(
                "{id}: {:?} {} until {}",
                wait.kind,
                wait.event_type.unwrap_or_default(),
                wait.wake_at.unwrap_or_default()
            );
            // docs:hide
            waiting.push((id, wait.kind));
            // docs:show
        }
    }
    // docs:end guides/graphs/event-waits-and-timers/run
    assert_eq!(
        waiting,
        [("order-1", WaitKind::Event), ("order-2", WaitKind::Event)]
    );

    let mut paid = None;
    // docs:begin guides/graphs/event-waits-and-timers/signal
    let signal = EventSignal::new("payment.received")
        .with_override("paid", true)
        .with_override("payment_ref", "PAY-5531");
    if let GraphExecutionResult::Interrupted {
        wait, frozen_state, ..
    } = orders
        .signal("order-1", signal.clone(), store.clone())
        .await
    {
        let wait = wait.unwrap();
        println!(
            "order-1: paid ({}), now {:?} until {}",
            frozen_state.payment_ref.unwrap_or_default(),
            wait.kind,
            wait.wake_at.unwrap_or_default()
        );
        // docs:hide
        paid = Some((frozen_state.paid, wait.kind));
        // docs:show
    }
    // docs:end guides/graphs/event-waits-and-timers/signal
    assert_eq!(paid, Some((true, WaitKind::Timer)));

    // docs:begin guides/graphs/event-waits-and-timers/refused
    let wrong_type = orders
        .signal("order-2", EventSignal::new("refund.issued"), store.clone())
        .await;
    let approve = orders
        .resume("order-2", HumanSignal::approve("clerk"), store.clone())
        .await;
    let not_waiting = orders.signal("order-1", signal, store.clone()).await;
    // docs:end guides/graphs/event-waits-and-timers/refused
    assert_eq!(
        refusal(&wrong_type),
        "thread 'order-2' is not waiting for event 'refund.issued': it is waiting for event 'payment.received' at [AwaitPayment]"
    );
    assert_eq!(
        refusal(&approve),
        "thread 'order-2' is waiting for event 'payment.received' at [AwaitPayment]: only a signal of that event answers it"
    );
    assert_eq!(
        refusal(&not_waiting),
        "thread 'order-1' is not waiting for event 'payment.received': it is waiting for a timer at [AwaitPickup]"
    );

    let mut woke = Vec::new();
    // docs:begin guides/graphs/event-waits-and-timers/wake
    let now = start + chrono::Duration::hours(5);
    let due = store
        .list_threads(&ThreadQuery::default().due_by(now))
        .await
        .unwrap();
    let ids: Vec<_> = due.threads.iter().map(|t| t.thread_id.as_str()).collect();
    println!("Due: {}", ids.join(", "));

    for now in [
        start + chrono::Duration::hours(5),
        start + chrono::Duration::hours(25),
    ] {
        let report = orders.wake_due(store.clone(), now).await.unwrap();
        for woken in &report.woken {
            println!(
                "Woke {} ({:?}): {}",
                woken.thread_id,
                woken.reason,
                outcome(&woken.result)
            );
            // docs:hide
            woke.push((
                woken.thread_id.clone(),
                woken.reason,
                outcome(&woken.result),
            ));
            // docs:show
        }
    }
    // docs:end guides/graphs/event-waits-and-timers/wake
    assert_eq!(ids, ["order-1"]);
    assert_eq!(
        woke,
        [
            (
                "order-1".to_string(),
                WakeReason::Timer,
                "completed, shipped = true".to_string()
            ),
            (
                "order-2".to_string(),
                WakeReason::Timeout,
                "cancelled (Ended) at AwaitPayment".to_string()
            ),
        ]
    );
}

#[tokio::test]
async fn wake_one_thread() {
    let orders = order_graph();
    let store = Arc::new(InMemoryCheckpointStore::new());
    for id in ["order-1005", "order-3"] {
        let order = Order {
            order_id: id.into(),
            paid: false,
            payment_ref: None,
            shipped: false,
        };
        orders.run(id, order, store.clone()).await;
    }
    let paid = EventSignal::new("payment.received").with_override("paid", true);
    orders.signal("order-1005", paid, store.clone()).await;
    let now = Utc::now() + chrono::Duration::hours(5);

    // docs:begin guides/graphs/event-waits-and-timers/wake-one
    let (handle, mut chunks) =
        orders
            .clone()
            .resume_streaming("order-1005", LevelSignals::due_at(now), store.clone());
    // docs:end guides/graphs/event-waits-and-timers/wake-one
    let mut nodes = Vec::new();
    while let Some(chunk) = chunks.recv().await {
        nodes.push(chunk.node_name);
    }
    let result = handle.await.expect("the woken run");
    assert!(matches!(&result, GraphExecutionResult::Completed(order) if order.shipped));
    assert!(!nodes.is_empty() && nodes.iter().all(|node| &**node == "Ship"));

    // A thread that isn't due yet is refused, with nothing written.
    let early = orders
        .resume("order-3", LevelSignals::due_at(now), store.clone())
        .await;
    let message = refusal(&early);
    assert!(message.starts_with("thread 'order-3' is not due until "));
    assert!(message.ends_with(": it is waiting for event 'payment.received' at [AwaitPayment]"));
}

// docs:begin guides/graphs/event-waits-and-timers/outcome
fn outcome(result: &GraphExecutionResult<Order>) -> String {
    match result {
        GraphExecutionResult::Completed(order) => format!("completed, shipped = {}", order.shipped),
        GraphExecutionResult::Cancelled { reason, node, .. } => {
            format!("cancelled ({reason:?}) at {node}")
        }
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            format!("interrupted at {suspended_node}")
        }
        GraphExecutionResult::Failed(error) => format!("failed: {error:?}"),
    }
}
// docs:end guides/graphs/event-waits-and-timers/outcome
