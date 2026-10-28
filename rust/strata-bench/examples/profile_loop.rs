//! Runs one scenario in a loop, for a sampling profiler to watch
//! (`design/38-Rust-Executor-Overhead.md`): a line of no-op nodes, run again
//! and again on a multi-threaded runtime for a number of seconds. It prints
//! what a run and a node cost, so a profile can be checked against a figure.
//!
//!   cargo run --release -p strata-bench --example profile_loop -- [async|sync|inline] [nodes] [seconds]
//!
//! The defaults are `async 100 10`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use datalium_strata::checkpoint::CheckpointStore;
use datalium_strata::graph::CompileOptions;
use strata_bench::{Handler, LatestOnlyStore, completed, linear, work};

fn main() {
    let mut args = std::env::args().skip(1);
    let handler = match args.next().as_deref() {
        Some("sync") => Handler::Sync,
        Some("inline") => Handler::Inline,
        _ => Handler::Async,
    };
    let nodes: usize = args.next().and_then(|n| n.parse().ok()).unwrap_or(100);
    let seconds: u64 = args.next().and_then(|n| n.parse().ok()).unwrap_or(10);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let store: Arc<dyn CheckpointStore> = LatestOnlyStore::new();
    let graph = linear(nodes, handler, CompileOptions::new());
    let (started, mut runs) = (Instant::now(), 0u64);
    rt.block_on(async {
        while started.elapsed() < Duration::from_secs(seconds) {
            for _ in 0..100 {
                completed(graph.run("bench", work(0), store.clone()).await);
            }
            runs += 100;
        }
    });
    let per_run = started.elapsed().as_nanos() as f64 / runs as f64;
    println!(
        "{} x{nodes}: {:.1} us a run, {:.0} ns a node",
        handler.name(),
        per_run / 1000.0,
        per_run / nodes as f64
    );
}
