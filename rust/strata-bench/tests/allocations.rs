//! What a node and a branch cost the heap (`design/38-Rust-Executor-Overhead.md`,
//! `design/49-Burst-Throughput.md`): the engine once made 28 allocations for
//! every node it ran, and that was a third of a node's time; a burst of runs
//! in flight at once holds what their branches allocate. This counts them, so
//! they don't come back unnoticed. A count, unlike a timing, is the same on
//! every machine.
//!
//!   cargo test -p strata-bench --test allocations -- --nocapture   # prints the counts
//!
//! One test in a binary of its own: the counter is the process's allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datalium_strata::checkpoint::CheckpointStore;
use datalium_strata::graph::{CompileOptions, CompiledGraph};
use strata_bench::{Handler, LatestOnlyStore, Work, completed, group, linear, work};

struct Counting;

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The allocations one run of a line of `nodes` nodes makes, once the
/// process is warm: the fewest of five runs, since tokio now and then starts
/// another blocking thread for a sync handler, and that allocates too.
async fn allocations(nodes: usize, handler: Handler, store: &Arc<dyn CheckpointStore>) -> u64 {
    allocations_of(&linear(nodes, handler, CompileOptions::new()), store).await
}

/// The allocations one run of `graph` makes, once the process is warm: the
/// fewest of five.
async fn allocations_of(graph: &CompiledGraph<Work>, store: &Arc<dyn CheckpointStore>) -> u64 {
    for _ in 0..50 {
        completed(graph.run("t", work(0), store.clone()).await);
    }
    let mut fewest = u64::MAX;
    for _ in 0..5 {
        let start = work(0);
        ALLOCATIONS.store(0, Ordering::SeqCst);
        COUNTING.store(true, Ordering::SeqCst);
        let end = graph.run("t", start, store.clone()).await;
        COUNTING.store(false, Ordering::SeqCst);
        completed(end);
        fewest = fewest.min(ALLOCATIONS.load(Ordering::SeqCst));
    }
    fewest
}

#[test]
fn a_node_allocates_almost_nothing() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let store: Arc<dyn CheckpointStore> = LatestOnlyStore::new();

    // (handler, allocations a node may make, allocations a run may make
    // besides its nodes'). An async node needs one: the future its handler
    // returns, boxed. So does an inline one (design/39-Inline-Sync-Handlers.md).
    // A sync one needs tokio's blocking task as well. The
    // limits leave room for a dependency to change by one, not for a string
    // per node.
    for (handler, per_node_limit, per_run_limit) in [
        (Handler::Async, 2.0, 16.0),
        (Handler::Sync, 4.0, 16.0),
        (Handler::Inline, 2.0, 16.0),
    ] {
        let (one, short, long) = runtime.block_on(async {
            (
                allocations(1, handler, &store).await,
                allocations(11, handler, &store).await,
                allocations(101, handler, &store).await,
            )
        });

        let per_node = (long - short) as f64 / 90.0;
        let per_run = one as f64 - per_node;
        println!(
            "{}: {per_node:.1} allocations a node, {per_run:.0} a run",
            handler.name()
        );
        assert!(
            per_node <= per_node_limit,
            "a {} node made {per_node:.1} allocations; the limit is {per_node_limit}",
            handler.name()
        );
        assert!(
            per_run <= per_run_limit,
            "a {} run made {per_run:.0} allocations besides its nodes'; the limit is {per_run_limit}",
            handler.name()
        );
    }

    // A parallel group's async branch (design/49-Burst-Throughput.md). Without
    // a branch timeout it watches its siblings' token, so it has no token of
    // its own, and its task holds no timer.
    let (one, nine) = runtime.block_on(async {
        (
            allocations_of(&group(1), &store).await,
            allocations_of(&group(9), &store).await,
        )
    });
    let per_branch = (nine - one) as f64 / 8.0;
    println!("async branch: {per_branch:.1} allocations a branch");
    assert!(
        per_branch <= PER_BRANCH_LIMIT,
        "an async branch made {per_branch:.1} allocations; the limit is {PER_BRANCH_LIMIT}"
    );
}

/// The allocations an async branch may make. It makes about 9 (12 before
/// `design/49`): its handler's future, its task and the task's place in the
/// group's `JoinSet`, its path, its context and span, and its name in its
/// outcome. The limit leaves room for a dependency to change by one, not for
/// a token or a copy of its name per branch.
const PER_BRANCH_LIMIT: f64 = 10.0;
