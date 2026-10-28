//! The engine's microbenchmarks (`design/37-Benchmarks.md`, SG-27): what a
//! run costs when the handlers do nothing and the store costs nothing, so the
//! time is the engine's. The .NET twin is
//! `dotnet/benchmarks/Datalium.Strata.Benchmarks/EngineBenchmarks.cs` — keep
//! the two in step.
//!
//!   cargo bench -p strata-bench                 # every benchmark
//!   cargo bench -p strata-bench -- traversal    # one group
//!   cargo bench -p strata-bench -- --test       # run each once, measure nothing (CI)

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datalium_strata::checkpoint::{CheckpointStore, HumanSignal};
use datalium_strata::graph::CompileOptions;
use strata_bench::{
    Handler, LatestOnlyStore, PAYLOAD_BYTES, RUN_NODES, approval, completed, durable, group,
    interrupted, linear, map, streamer, work,
};
use tokio::runtime::Runtime;

/// The runtime an application has: multi-threaded, with the default workers.
fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

fn store() -> Arc<dyn CheckpointStore> {
    LatestOnlyStore::new()
}

/// A run of a line of no-op nodes: the per-node cost is the time over the
/// node count (criterion reports it as elements per second).
fn traversal(c: &mut Criterion) {
    let (rt, store) = (runtime(), store());
    let mut benchmarks = c.benchmark_group("traversal");
    for handler in Handler::ALL {
        for nodes in [1, 10, 100] {
            let graph = linear(nodes, handler, CompileOptions::new());
            benchmarks.throughput(Throughput::Elements(nodes as u64));
            benchmarks.bench_function(BenchmarkId::new(handler.name(), nodes), |b| {
                b.to_async(&rt).iter(|| async {
                    black_box(completed(graph.run("bench", work(0), store.clone()).await))
                });
            });
        }
    }
    benchmarks.finish();
}

/// The same ten-node run with an empty state and a 16 KiB one: what a state's
/// size costs a run that doesn't checkpoint. A run takes its state by value, so
/// each iteration clones the one built outside it.
fn state_size(c: &mut Criterion) {
    let (rt, store) = (runtime(), store());
    let graph = linear(RUN_NODES, Handler::Async, CompileOptions::new());
    let mut benchmarks = c.benchmark_group("state_size");
    for bytes in [0, 16 * 1024] {
        let start = work(bytes);
        benchmarks.bench_function(BenchmarkId::new("async", bytes), |b| {
            b.to_async(&rt).iter(|| async {
                black_box(completed(
                    graph.run("bench", start.clone(), store.clone()).await,
                ))
            });
        });
    }
    benchmarks.finish();
}

/// A parallel group of no-op branches, and a map over no-op children.
fn fan_out(c: &mut Criterion) {
    let (rt, store) = (runtime(), store());
    let mut benchmarks = c.benchmark_group("fan_out");
    for branches in [2, 8, 32] {
        let graph = group(branches);
        benchmarks.throughput(Throughput::Elements(branches as u64));
        benchmarks.bench_function(BenchmarkId::new("group", branches), |b| {
            b.to_async(&rt).iter(|| async {
                black_box(completed(graph.run("bench", work(0), store.clone()).await))
            });
        });
    }
    for items in [8, 64] {
        let graph = map(items);
        benchmarks.throughput(Throughput::Elements(items as u64));
        benchmarks.bench_function(BenchmarkId::new("map", items), |b| {
            b.to_async(&rt).iter(|| async {
                black_box(completed(graph.run("bench", work(0), store.clone()).await))
            });
        });
    }
    benchmarks.finish();
}

/// A run to an interrupt boundary and the resume that finishes it: freeze,
/// flush, evict, rehydrate, with a 1 KiB state.
fn interrupt(c: &mut Criterion) {
    let (rt, store) = (runtime(), store());
    let graph = approval();
    let start = work(PAYLOAD_BYTES);
    c.benchmark_group("interrupt")
        .bench_function("round_trip", |b| {
            b.to_async(&rt).iter(|| async {
                interrupted(graph.run("bench", start.clone(), store.clone()).await);
                black_box(completed(
                    graph
                        .resume("bench", HumanSignal::approve("bench"), store.clone())
                        .await,
                ))
            });
        });
}

/// The ten-node run with a checkpoint at boundaries only (none here) and
/// after every node: what `EveryNode` costs the engine before any store's I/O.
fn checkpointing(c: &mut Criterion) {
    let (rt, store) = (runtime(), store());
    let mut benchmarks = c.benchmark_group("checkpointing");
    let start = work(PAYLOAD_BYTES);
    let boundaries = linear(RUN_NODES, Handler::Async, CompileOptions::new());
    benchmarks.bench_function("boundaries", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(completed(
                boundaries.run("bench", start.clone(), store.clone()).await,
            ))
        });
    });
    let every_node = durable();
    benchmarks.bench_function("every_node", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(completed(
                every_node.run("bench", start.clone(), store.clone()).await,
            ))
        });
    });
    benchmarks.finish();
}

/// One node emitting a thousand tokens to a reader that drains them.
fn streaming(c: &mut Criterion) {
    const TOKENS: usize = 1000;
    let (rt, store) = (runtime(), store());
    let graph = Arc::new(streamer(TOKENS));
    let mut benchmarks = c.benchmark_group("streaming");
    benchmarks.throughput(Throughput::Elements(TOKENS as u64));
    benchmarks.bench_function(BenchmarkId::new("tokens", TOKENS), |b| {
        b.to_async(&rt).iter(|| async {
            let (handle, mut chunks) = graph.clone().run_streaming("bench", work(0), store.clone());
            let mut seen = 0usize;
            while chunks.recv().await.is_some() {
                seen += 1;
            }
            assert!(seen >= TOKENS);
            black_box(completed(handle.await.expect("the run's task")))
        });
    });
    benchmarks.finish();
}

criterion_group!(
    engine,
    traversal,
    state_size,
    fan_out,
    interrupt,
    checkpointing,
    streaming
);
criterion_main!(engine);
