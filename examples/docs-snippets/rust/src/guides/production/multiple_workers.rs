//! Samples for <https://stratamesh.dev/guides/production/multiple-workers/>.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use datalium_strata::checkpoint::{CancelReason, GraphExecutionResult, LeaseOptions};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::scheduler::{RunScheduler, SchedulerOptions};
use datalium_strata::store::{CheckpointQuery, CheckpointStore, ThreadQuery, ThreadState};
use datalium_strata::{CancellationToken, StrataError};
use datalium_strata_memory::checkpoint::{InMemoryCheckpointStore, SqliteCheckpointStore};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

// The samples print what they find. This `println!` keeps the lines instead, so the tests can check them.
// Each test runs on one thread (tokio's current-thread runtime), so the lines are kept per thread.
thread_local! {
    static PRINTED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

macro_rules! println {
    ($($arg:tt)*) => {
        PRINTED.with(|printed| printed.borrow_mut().push(format!($($arg)*)))
    };
}

fn printed() -> Vec<String> {
    PRINTED.with(|printed| std::mem::take(&mut *printed.borrow_mut()))
}

const TTL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Invoice {
    invoice_id: String,
    drafted_by: Option<String>,
    sent: bool,
}

impl Invoice {
    fn new(id: &str) -> Self {
        Self {
            invoice_id: id.to_string(),
            drafted_by: None,
            sent: false,
        }
    }
}

/// Draft -> (wait for approval) -> Send, compiled with `EveryNode` and the graph id `invoices`. Draft waits
/// for `gate`, when there is one, ignoring its token like a paused process.
fn invoices(owner: &str, gate: Option<Gate>) -> CompiledGraph<Invoice> {
    let owner = owner.to_string();
    StateGraph::<Invoice>::new()
        .add_async_node("Draft", move |invoice: Invoice, _: NodeContext| {
            let (owner, gate) = (owner.clone(), gate.clone());
            async move {
                if let Some(gate) = gate {
                    gate.hold().await;
                }
                Invoice {
                    drafted_by: Some(owner),
                    ..invoice
                }
            }
        })
        .add_interrupt_boundary("Draft")
        .add_node("Send", |invoice: Invoice| Invoice {
            sent: true,
            ..invoice
        })
        .add_edge("Draft", "Send")
        .compile_with(
            CompileOptions::new()
                .with_checkpointing(CheckpointPolicy::EveryNode)
                .with_graph_id("invoices"),
        )
        .expect("the invoice graph is valid")
}

/// Holds Draft until released.
#[derive(Clone, Default)]
struct Gate {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Gate {
    async fn hold(&self) {
        self.started.notify_one();
        self.release.notified().await;
    }

    async fn started(&self) {
        self.started.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

fn noon() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 24, 12, 0, 0).unwrap()
}

/// The store's clock, the one lease expiry is judged by: noon when the test starts, moving with tokio's
/// paused clock plus whatever `ahead` adds. Time that passes for the store alone is a stalled worker:
/// its renewals run on tokio's clock, so they don't fire.
#[derive(Clone)]
struct StoreClock {
    started: tokio::time::Instant,
    ahead: Arc<Mutex<Duration>>,
}

impl StoreClock {
    fn new() -> Self {
        Self {
            started: tokio::time::Instant::now(),
            ahead: Arc::default(),
        }
    }

    fn clock(&self) -> datalium_strata_memory::Clock {
        let clock = self.clone();
        Arc::new(move || {
            let elapsed = clock.started.elapsed() + *clock.ahead.lock().unwrap();
            noon() + chrono::Duration::from_std(elapsed).unwrap()
        })
    }

    fn advance_store_alone(&self, by: Duration) {
        *self.ahead.lock().unwrap() += by;
    }
}

struct Worker {
    owner: String,
    graph: CompiledGraph<Invoice>,
    store: Arc<InMemoryCheckpointStore>,
}

impl Worker {
    fn new(owner: &str, store: &Arc<InMemoryCheckpointStore>, gate: Option<Gate>) -> Arc<Self> {
        Arc::new(Self {
            owner: owner.to_string(),
            graph: invoices(owner, gate),
            store: store.clone(),
        })
    }

    // docs:begin guides/production/multiple-workers/handle
    async fn handle(&self, thread_id: &str) {
        let config = RunConfig::new().with_lease(LeaseOptions::new(self.owner.clone(), TTL));
        let result = self
            .graph
            .run_with(
                thread_id,
                Invoice::new(thread_id),
                self.store.clone(),
                config,
            )
            .await;

        match result {
            GraphExecutionResult::Failed(StrataError::ExecutionAborted(why))
                if why.contains(" is held by ") =>
            {
                // Another worker runs this thread. Nothing was written.
                println!("{}: skipped {thread_id}: {why}", self.owner);
            }
            GraphExecutionResult::Cancelled {
                reason: CancelReason::LeaseLost,
                node,
                ..
            } => {
                // Another worker took the thread over. This run wrote nothing more.
                println!("{}: lost the lease on {thread_id} in {node}", self.owner);
            }
            GraphExecutionResult::Interrupted { suspended_node, .. } => {
                println!("{}: {thread_id} waits at {suspended_node}", self.owner);
            }
            _ => println!("{}: {thread_id} ended another way", self.owner),
        }
    }
    // docs:end guides/production/multiple-workers/handle
}

// A current-thread runtime with its clock paused: time passes only when nothing else can run.
#[tokio::test(start_paused = true)]
async fn handle() {
    let clock = StoreClock::new();
    let store = Arc::new(InMemoryCheckpointStore::new().with_clock(clock.clock()));

    // Held: worker-a is in Draft on invoice-7, and worker-b gets the same thread.
    let gate = Gate::default();
    let a = Worker::new("worker-a", &store, Some(gate.clone()));
    let b = Worker::new("worker-b", &store, None);
    let a_run = tokio::spawn({
        let a = a.clone();
        async move { a.handle("invoice-7").await }
    });
    gate.started().await;
    b.handle("invoice-7").await;
    gate.release();
    a_run.await.expect("worker-a's run");
    assert_eq!(
        printed(),
        [
            "worker-b: skipped invoice-7: thread 'invoice-7' is held by 'worker-a' until 2026-10-24T12:00:03Z",
            "worker-a: invoice-7 waits at Draft",
        ]
    );

    // Lost: worker-a stalls in Draft on invoice-8 past its ttl, worker-b takes the thread, and worker-a's
    // save is refused when it wakes up.
    let gate = Gate::default();
    let a = Worker::new("worker-a", &store, Some(gate.clone()));
    let a_run = tokio::spawn({
        let a = a.clone();
        async move { a.handle("invoice-8").await }
    });
    gate.started().await;
    clock.advance_store_alone(TTL + Duration::from_secs(1));
    b.handle("invoice-8").await;
    gate.release();
    a_run.await.expect("worker-a's run");
    assert_eq!(
        printed(),
        [
            "worker-b: invoice-8 waits at Draft",
            "worker-a: lost the lease on invoice-8 in Draft",
        ]
    );
    let latest = store
        .load_latest("invoice-8")
        .await
        .expect("the store answers")
        .expect("invoice-8 has a checkpoint")
        .checkpoint;
    assert_eq!(latest.suspended_node.as_deref(), Some("Draft"));
    assert_eq!(latest.frozen_state["drafted_by"], "worker-b");
}

// docs:begin guides/production/multiple-workers/show-running
async fn show_running(store: &SqliteCheckpointStore) {
    let mut query = ThreadQuery::default()
        .with_states([ThreadState::Running])
        .with_graph("invoices");
    loop {
        let page = store.list_threads(&query).await.expect("list threads");
        for thread in &page.threads {
            match store
                .get_lease(&thread.thread_id)
                .await
                .expect("read the lease")
            {
                Some(lease) => println!(
                    "{}: running, held by '{}' until {} (token {})",
                    thread.thread_id,
                    lease.owner,
                    lease.expires_at.format("%H:%M:%S"),
                    lease.token
                ),
                None => println!("{}: running, no live lease", thread.thread_id),
            }
        }
        match page.next_cursor {
            Some(cursor) => query = query.with_cursor(cursor),
            None => break,
        }
    }
}
// docs:end guides/production/multiple-workers/show-running

#[tokio::test]
async fn show_running_threads() {
    let store = Arc::new(SqliteCheckpointStore::open_in_memory().expect("the store opens"));
    let leased =
        RunConfig::new().with_lease(LeaseOptions::new("worker-a", Duration::from_secs(30)));

    // invoice-7 is in Draft under worker-a's lease, and invoice-9 in Draft with no lease.
    let (held, unleased) = (Gate::default(), Gate::default());
    let runs = [
        ("invoice-7", held.clone(), leased.clone()),
        ("invoice-9", unleased.clone(), RunConfig::new()),
    ]
    .map(|(thread, gate, config)| {
        let (graph, store) = (invoices("worker-a", Some(gate)), store.clone());
        tokio::spawn(async move {
            graph
                .run_with(thread, Invoice::new(thread), store, config)
                .await
        })
    });
    held.started().await;
    unleased.started().await;
    // invoice-10 waits for approval: not running.
    let waiting = invoices("worker-b", None)
        .run_with(
            "invoice-10",
            Invoice::new("invoice-10"),
            store.clone(),
            leased.clone(),
        )
        .await;
    assert!(matches!(waiting, GraphExecutionResult::Interrupted { .. }));
    // A running thread of another graph.
    let other = Gate::default();
    let other_run = {
        let gate = other.clone();
        let graph = StateGraph::<Invoice>::new()
            .add_async_node("Draft", move |invoice: Invoice, _: NodeContext| {
                let gate = gate.clone();
                async move {
                    gate.hold().await;
                    invoice
                }
            })
            .compile_with(
                CompileOptions::new()
                    .with_checkpointing(CheckpointPolicy::EveryNode)
                    .with_graph_id("receipts"),
            )
            .expect("the receipt graph is valid");
        let store = store.clone();
        tokio::spawn(async move {
            graph
                .run_with(
                    "receipt-1",
                    Invoice::new("receipt-1"),
                    store,
                    RunConfig::new(),
                )
                .await
        })
    };
    other.started().await;

    show_running(&store).await;

    let mut lines = printed();
    lines.sort();
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(
        lines[0].starts_with("invoice-7: running, held by 'worker-a' until ")
            && lines[0].ends_with(" (token 1)"),
        "{lines:?}"
    );
    assert_eq!(lines[1], "invoice-9: running, no live lease");

    held.release();
    unleased.release();
    other.release();
    for run in runs {
        let result = run.await.expect("the run's task");
        assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    }
    other_run.await.expect("the receipt run's task");
}

#[tokio::test]
async fn scheduler() {
    // docs:begin guides/production/multiple-workers/scheduler
    let scheduler = RunScheduler::new(
        SchedulerOptions::new(8)
            .with_tenant_limit(2)
            .with_reservation(5, 2),
    )
    .expect("valid scheduler options");

    let config = RunConfig::new()
        .with_lease(LeaseOptions::new("worker-a", Duration::from_secs(30)))
        .with_scheduler(scheduler.clone())
        .with_priority(5)
        .with_metadata("tenant", "acme");
    // docs:end guides/production/multiple-workers/scheduler

    // Three of acme's runs at once: two hold slots, and the third waits for one.
    let store = Arc::new(InMemoryCheckpointStore::new());
    let open = CancellationToken::new();
    let graph = Arc::new(
        StateGraph::<Invoice>::new()
            .add_async_node("Draft", {
                let open = open.clone();
                move |invoice: Invoice, _: NodeContext| {
                    let open = open.clone();
                    async move {
                        open.cancelled().await;
                        invoice
                    }
                }
            })
            .compile()
            .expect("the graph is valid"),
    );
    let runs: Vec<_> = (1..=3)
        .map(|n| {
            let (graph, store, config) = (graph.clone(), store.clone(), config.clone());
            tokio::spawn(async move {
                let thread = format!("scheduled-{n}");
                let invoice = Invoice::new(&thread);
                graph.run_with(&thread, invoice, store, config).await
            })
        })
        .collect();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let snapshot = loop {
        let snapshot = scheduler.snapshot();
        if snapshot.active + snapshot.queued == 3 {
            break snapshot;
        }
        assert!(tokio::time::Instant::now() < deadline, "{snapshot:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(
        (snapshot.max_concurrent, snapshot.active, snapshot.queued),
        (8, 2, 1)
    );
    let acme = snapshot.tenants["acme"];
    assert_eq!((acme.active, acme.limit), (2, Some(2)));

    open.cancel();
    for run in runs {
        let result = run.await.expect("the run's task");
        assert!(matches!(result, GraphExecutionResult::Completed(_)));
    }
}
