//! The scenarios Strata's benchmarks run (`design/37-Benchmarks.md`, SG-27):
//! the graphs, the state and a store that costs nothing. The criterion
//! microbenchmarks (`benches/engine.rs`) and the profile (`src/main.rs`) both
//! build on them, so a number in one is about the same graph as a number in
//! the other. The .NET twin is
//! `dotnet/benchmarks/Datalium.Strata.Benchmarks/Scenarios.cs` — keep the two
//! in step: same scenario names, same graphs, same parameters.

pub mod stats;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::{CheckpointStore, CheckpointStoreError, GraphExecutionResult};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::Checkpoint;
use serde::{Deserialize, Serialize};

/// The nodes of the graph the run-level scenarios use.
pub const RUN_NODES: usize = 10;

/// The payload of a state that stands for a real one: 1 KiB of text.
pub const PAYLOAD_BYTES: usize = 1024;

/// The state every scenario runs on: a counter each node adds one to, and a
/// payload that only gives the state a size (Rust clones it into every async
/// handler; a checkpoint serializes it).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Work {
    pub step: u64,
    pub payload: String,
}

/// A state at step 0 with `payload_bytes` of payload.
pub fn work(payload_bytes: usize) -> Work {
    Work {
        step: 0,
        payload: "x".repeat(payload_bytes),
    }
}

fn stepped(mut work: Work) -> Work {
    work.step += 1;
    work
}

/// Which kind of handler a scenario's nodes have. A sync handler runs on
/// tokio's blocking pool, an async one on the executor, and an inline one is
/// a sync handler on the executor (`add_inline_node`,
/// `design/39-Inline-Sync-Handlers.md`). Inline is Rust's alone: the .NET
/// catalogue has no such row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handler {
    Sync,
    Async,
    Inline,
}

impl Handler {
    pub const ALL: [Handler; 3] = [Handler::Async, Handler::Sync, Handler::Inline];

    pub fn name(self) -> &'static str {
        match self {
            Handler::Sync => "sync",
            Handler::Async => "async",
            Handler::Inline => "inline",
        }
    }
}

fn node_name(index: usize) -> String {
    format!("n{index}")
}

/// `nodes` nodes in a line, each adding one to the step: the cost of a run is
/// the engine's, since the handlers do nothing.
pub fn linear(nodes: usize, handler: Handler, options: CompileOptions) -> CompiledGraph<Work> {
    let mut graph = StateGraph::<Work>::new();
    for index in 0..nodes {
        graph = match handler {
            Handler::Sync => graph.add_node(node_name(index), stepped),
            Handler::Inline => graph.add_inline_node(node_name(index), stepped),
            Handler::Async => graph.add_async_node(
                node_name(index),
                |work: Work, _ctx| async move { stepped(work) },
            ),
        };
    }
    for index in 1..nodes {
        graph = graph.add_edge(node_name(index - 1), node_name(index));
    }
    graph.compile_with(options).expect("a line compiles")
}

/// [`linear`] with [`RUN_NODES`] async nodes that checkpoints after every
/// node (`design/33-Durable-Execution.md`).
pub fn durable() -> CompiledGraph<Work> {
    linear(
        RUN_NODES,
        Handler::Async,
        CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode),
    )
}

/// One parallel group of `branches` async branches, each adding one to the
/// step; the reducer adds up what the branches added.
pub fn group(branches: usize) -> CompiledGraph<Work> {
    let mut fan = ParallelBranches::<Work>::new();
    for index in 0..branches {
        fan = fan.async_branch(format!("b{index}"), |work: Work, _ctx| async move {
            Ok(stepped(work))
        });
    }
    StateGraph::<Work>::new()
        .add_parallel_group("fan", fan, |entry: &Work, results: Vec<Work>| Work {
            step: entry.step + results.iter().map(|r| r.step - entry.step).sum::<u64>(),
            payload: entry.payload.clone(),
        })
        .compile()
        .expect("a group compiles")
}

/// One map over `items` items, each run through a one-node child graph, at
/// the map's default concurrency (8); the reducer counts the children that
/// finished.
pub fn map(items: usize) -> CompiledGraph<Work> {
    let child = Arc::new(linear(1, Handler::Async, CompileOptions::new()));
    StateGraph::<Work>::new()
        .add_map(
            "each",
            move |_: &Work| (0..items as u64).collect::<Vec<u64>>(),
            |item: &u64| item.to_string(),
            child,
            |_: &Work, _item: u64| work(0),
            |entry: &Work, results: Vec<Work>| Work {
                step: entry.step + results.iter().map(|r| r.step).sum::<u64>(),
                payload: entry.payload.clone(),
            },
        )
        .compile()
        .expect("a map compiles")
}

/// `prepare`, then `approve` (an interrupt boundary), then `finish`: a run
/// freezes after `approve`, and a resume runs `finish`.
pub fn approval() -> CompiledGraph<Work> {
    StateGraph::<Work>::new()
        .add_async_node("prepare", |work: Work, _ctx| async move { stepped(work) })
        .add_async_node("approve", |work: Work, _ctx| async move { stepped(work) })
        .add_async_node("finish", |work: Work, _ctx| async move { stepped(work) })
        .add_edge("prepare", "approve")
        .add_edge("approve", "finish")
        .add_interrupt_boundary("approve")
        .compile()
        .expect("an approval compiles")
}

/// One streaming node that emits `tokens` tokens.
pub fn streamer(tokens: usize) -> CompiledGraph<Work> {
    StateGraph::<Work>::new()
        .add_async_streaming_node("speak", move |work: Work, ctx: NodeContext| async move {
            let stream = ctx.stream().expect("a streaming run").clone();
            for _ in 0..tokens {
                stream.emit_token("token").await.expect("a reader");
            }
            stepped(work)
        })
        .compile()
        .expect("a streamer compiles")
}

/// What the engine does between one handler returning and the next one
/// starting, measured from inside the handlers: each notes the time it starts
/// and the time it returns, and the gap from one node's return to the next
/// node's start is a sample. The first node of a run has no gap.
pub struct Gaps {
    base: Instant,
    last_exit: AtomicU64,
    samples: Mutex<Vec<u64>>,
}

impl Gaps {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            base: Instant::now(),
            last_exit: AtomicU64::new(0),
            samples: Mutex::new(Vec::new()),
        })
    }

    fn now(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    fn enter(&self, first: bool) {
        let now = self.now();
        if !first {
            let gap = now.saturating_sub(self.last_exit.load(Ordering::Relaxed));
            self.samples.lock().unwrap().push(gap);
        }
    }

    fn exit(&self) {
        self.last_exit.store(self.now(), Ordering::Relaxed);
    }

    /// The gaps recorded since the last call, in nanoseconds.
    pub fn take(&self) -> Vec<u64> {
        std::mem::take(&mut self.samples.lock().unwrap())
    }
}

/// [`linear`] whose handlers record the gap between them in `gaps`.
pub fn timed(nodes: usize, handler: Handler, gaps: &Arc<Gaps>) -> CompiledGraph<Work> {
    fn step(work: Work, gaps: &Gaps) -> Work {
        gaps.enter(work.step == 0);
        let work = stepped(work);
        gaps.exit();
        work
    }

    let mut graph = StateGraph::<Work>::new();
    for index in 0..nodes {
        let gaps = gaps.clone();
        graph = match handler {
            Handler::Sync => graph.add_node(node_name(index), move |work| step(work, &gaps)),
            Handler::Inline => {
                graph.add_inline_node(node_name(index), move |work| step(work, &gaps))
            }
            Handler::Async => graph.add_async_node(node_name(index), move |work: Work, _ctx| {
                let gaps = gaps.clone();
                async move { step(work, &gaps) }
            }),
        };
    }
    for index in 1..nodes {
        graph = graph.add_edge(node_name(index - 1), node_name(index));
    }
    graph.compile().expect("a line compiles")
}

/// A checkpoint store that keeps a thread's latest checkpoint and nothing
/// else, in memory, and counts the saves. The engine's own cost is what is
/// left when the store costs this little, and a thread run a million times
/// doesn't grow a log.
#[derive(Default)]
pub struct LatestOnlyStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
    saves: AtomicU64,
}

impl LatestOnlyStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The checkpoints saved so far.
    pub fn saves(&self) -> u64 {
        self.saves.load(Ordering::Relaxed)
    }

    /// `thread_id`'s latest checkpoint.
    pub fn latest(&self, thread_id: &str) -> Option<Checkpoint> {
        self.checkpoints.lock().unwrap().get(thread_id).cloned()
    }
}

impl CheckpointStore for LatestOnlyStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.saves.fetch_add(1, Ordering::Relaxed);
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self.latest(thread_id))))
    }
}

/// The final state of a run that completed; panics on any other outcome, so
/// a benchmark can't time a run that failed.
pub fn completed(result: GraphExecutionResult<Work>) -> Work {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            panic!("the run froze at '{suspended_node}'")
        }
        GraphExecutionResult::Failed(error) => panic!("the run failed: {error:?}"),
        GraphExecutionResult::Cancelled { node, reason, .. } => {
            panic!("the run was cancelled at '{node}': {reason:?}")
        }
    }
}

/// Panics unless a run froze at an interrupt boundary.
pub fn interrupted(result: GraphExecutionResult<Work>) {
    match result {
        GraphExecutionResult::Interrupted { .. } => {}
        GraphExecutionResult::Completed(_) => panic!("the run completed"),
        GraphExecutionResult::Failed(error) => panic!("the run failed: {error:?}"),
        GraphExecutionResult::Cancelled { node, reason, .. } => {
            panic!("the run was cancelled at '{node}': {reason:?}")
        }
    }
}

/// An `interrupted` checkpoint of [`approval`] with a [`PAYLOAD_BYTES`]
/// state: what the store scenarios save, under one thread id after another.
pub async fn sample_checkpoint() -> Checkpoint {
    let store = LatestOnlyStore::new();
    interrupted(
        approval()
            .run("sample", work(PAYLOAD_BYTES), store.clone())
            .await,
    );
    store.latest("sample").expect("a checkpoint")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalium_strata::checkpoint::HumanSignal;
    use datalium_strata::schema::CheckpointStatus;
    use datalium_strata::streaming::StreamChunkType;

    fn store() -> Arc<LatestOnlyStore> {
        LatestOnlyStore::new()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_line_runs_every_node_once_with_every_kind_of_handler() {
        for handler in Handler::ALL {
            for nodes in [1, 10, 100] {
                let graph = linear(nodes, handler, CompileOptions::new());
                let store = store();

                let end = completed(graph.run("t", work(0), store.clone()).await);

                assert_eq!(end.step, nodes as u64, "{} x{nodes}", handler.name());
                assert_eq!(store.saves(), 0, "a run with no boundary saves nothing");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_durable_line_saves_a_checkpoint_per_node_and_one_more() {
        let store = store();

        let end = completed(durable().run("t", work(0), store.clone()).await);

        assert_eq!(end.step, RUN_NODES as u64);
        // `running` before the first node and after each node but the last, whose commit is `completed`.
        assert_eq!(store.saves(), RUN_NODES as u64 + 1);
        assert_eq!(
            store.latest("t").unwrap().status,
            CheckpointStatus::Completed
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_group_runs_every_branch_and_a_map_every_item() {
        for branches in [2, 8, 32] {
            let end = completed(group(branches).run("t", work(0), store()).await);
            assert_eq!(end.step, branches as u64);
        }
        for items in [8, 64] {
            let end = completed(map(items).run("t", work(0), store()).await);
            assert_eq!(end.step, items as u64);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_approval_freezes_once_and_a_resume_finishes_it() {
        let (graph, store) = (approval(), store());

        interrupted(graph.run("t", work(PAYLOAD_BYTES), store.clone()).await);
        assert_eq!(store.saves(), 1);
        let end = completed(
            graph
                .resume("t", HumanSignal::approve("bench"), store.clone())
                .await,
        );

        assert_eq!(end.step, 3);
        // The same thread id runs again from the start: what the round-trip benchmark does.
        interrupted(graph.run("t", work(0), store.clone()).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_streamer_emits_its_tokens() {
        let graph = Arc::new(streamer(1000));

        let (handle, mut chunks) = graph.run_streaming("t", work(0), store());
        let mut tokens = 0;
        while let Some(chunk) = chunks.recv().await {
            if chunk.chunk_type == StreamChunkType::Token {
                tokens += 1;
            }
        }

        assert_eq!(tokens, 1000);
        assert_eq!(completed(handle.await.unwrap()).step, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_timed_line_records_a_gap_for_every_transition() {
        for handler in Handler::ALL {
            let gaps = Gaps::new();
            let graph = timed(100, handler, &gaps);

            completed(graph.run("t", work(0), store()).await);
            completed(graph.run("t", work(0), store()).await);

            assert_eq!(gaps.take().len(), 2 * 99, "{}", handler.name());
            assert!(gaps.take().is_empty());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_sample_checkpoint_is_an_interrupted_one_with_the_payload() {
        let checkpoint = sample_checkpoint().await;

        assert_eq!(checkpoint.status, CheckpointStatus::Interrupted);
        assert_eq!(checkpoint.suspended_node.as_deref(), Some("approve"));
        assert!(serde_json::to_string(&checkpoint).unwrap().len() > PAYLOAD_BYTES);
    }
}
