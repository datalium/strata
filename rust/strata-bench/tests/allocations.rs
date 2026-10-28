//! What a node costs the heap (`design/38-Rust-Executor-Overhead.md`): the
//! engine once made 28 allocations for every node it ran, and that was a third
//! of a node's time. This counts them, so they don't come back unnoticed. A
//! count, unlike a timing, is the same on every machine.
//!
//!   cargo test -p strata-bench --test allocations -- --nocapture   # prints the counts
//!
//! One test in a binary of its own: the counter is the process's allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datalium_strata::checkpoint::CheckpointStore;
use datalium_strata::graph::CompileOptions;
use strata_bench::{Handler, LatestOnlyStore, completed, linear, work};

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
    let graph = linear(nodes, handler, CompileOptions::new());
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
}
