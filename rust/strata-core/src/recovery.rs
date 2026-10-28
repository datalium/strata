//! Recovering threads whose run died (`design/33-Durable-Execution.md`,
//! SG-10): the options and the report of
//! [`CompiledGraph::recover_stale`](crate::graph::CompiledGraph::recover_stale),
//! and the count a recovery's claim records. The recovery itself is in
//! [`crate::checkpoint`], with the rest of the executor.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::checkpoint::GraphExecutionResult;
use crate::context::RunConfig;
use crate::schema::Checkpoint;
use crate::store::DEFAULT_PAGE_SIZE;
pub use crate::wait::SkippedThread;

/// The checkpoint `metadata` key a recovery's claim records its count under,
/// on the outermost level: how many times the thread has been recovered from
/// the checkpoint it stands at. Only a claim carries it, so it is a count of
/// recoveries since a node last committed. Mirrors the .NET engine's
/// `RecoveryCount.MetadataKey`.
pub const RECOVERIES_KEY: &str = "strata.recoveries";

/// The count `checkpoint` records, or 0 when it records none.
///
/// [`RecoverOptions::with_max_recoveries`] limits the count for
/// [`recover_stale`](crate::graph::CompiledGraph::recover_stale);
/// `recover`, `recover_with` and the streaming forms always recover, so an
/// application that recovers a thread itself reads this on the thread's
/// latest checkpoint and stops at its own limit.
pub fn recoveries_of(checkpoint: &Checkpoint) -> u32 {
    checkpoint
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(RECOVERIES_KEY))
        .filter(|stored| !stored.is_empty() && stored.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|stored| stored.parse().ok())
        .unwrap_or(0)
}

/// Where a run of `checkpoint` continues: each level's next node, from the
/// outermost in.
pub(crate) fn continues_at(checkpoint: &Checkpoint) -> Vec<String> {
    let mut path = Vec::new();
    let mut level = Some(checkpoint);
    while let Some(current) = level {
        if let Some(node) = &current.next_node {
            path.push(node.clone());
        }
        level = current.nested.as_deref();
    }
    path
}

/// Options for
/// [`CompiledGraph::recover_stale`](crate::graph::CompiledGraph::recover_stale)
/// (`design/33-Durable-Execution.md`, "Who is dead"): which `running` threads
/// count as dead, how many to recover, and how each recovery runs. Mirrors the
/// .NET engine's `RecoverOptions`.
#[derive(Clone)]
pub struct RecoverOptions {
    pub(crate) stale_after: Option<Duration>,
    pub(crate) max_recoveries: Option<u32>,
    pub(crate) metadata: BTreeMap<String, String>,
    pub(crate) match_graph: bool,
    pub(crate) limit: usize,
    pub(crate) config: RunConfig,
    pub(crate) stop: Option<CancellationToken>,
}

impl Default for RecoverOptions {
    fn default() -> Self {
        Self {
            stale_after: None,
            max_recoveries: None,
            metadata: BTreeMap::new(),
            match_graph: true,
            limit: DEFAULT_PAGE_SIZE,
            config: RunConfig::default(),
            stop: None,
        }
    }
}

impl RecoverOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// How old a thread's latest `running` checkpoint must be before the
    /// thread is recovered. A thread a live lease holds is never recovered,
    /// whatever its age.
    ///
    /// Not set (the default) is zero when [`Self::with_config`]'s config has a
    /// lease: every worker then runs under one, and a `running` thread nobody
    /// holds has no run. Without a lease it must be set, to more than the
    /// longest a node can take: the age of the checkpoint is then the only
    /// sign of life, a long node ages it too, and a node slower than this is
    /// run twice.
    pub fn with_stale_after(mut self, age: Duration) -> Self {
        self.stale_after = Some(age);
        self
    }

    /// The most times a thread is recovered without a node committing in
    /// between ([`RECOVERIES_KEY`] on its latest checkpoint): a thread already
    /// recovered this often is skipped, and stays `running` for an operator to
    /// look at and end. Stops a node that kills its process from being run
    /// forever. No limit by default. At least 1.
    pub fn with_max_recoveries(mut self, limit: u32) -> Self {
        self.max_recoveries = Some(limit);
        self
    }

    /// Only threads whose latest checkpoint's metadata has this pair (and
    /// every other one added): a store holds many graphs' threads.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Whether to list only this graph's threads, when it has an identity
    /// (`design/31-Graph-Identity.md`): its id, and its version unless it has
    /// a migration. `true` by default; `false` lists every graph's, and each
    /// recovery still refuses a thread that isn't this graph's.
    pub fn with_match_graph(mut self, match_graph: bool) -> Self {
        self.match_graph = match_graph;
        self
    }

    /// The most threads recovered in one call: 1 to
    /// [`MAX_PAGE_SIZE`](crate::store::MAX_PAGE_SIZE), 100 by default. Threads
    /// that are skipped don't count, so live threads can't crowd out dead
    /// ones.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// The config of each recovery, which is a run of its own: its lease
    /// (every worker's, or none's), its metadata, observer and services, and
    /// the token that is also checked between threads.
    pub fn with_config(mut self, config: RunConfig) -> Self {
        self.config = config;
        self
    }

    /// When `stop` fires, the call recovers no more threads and returns what
    /// it did; the recovery in flight finishes. Unlike the config's
    /// cancellation token, which also cancels that run. How a host stops a
    /// driver without leaving a thread cancelled
    /// (`design/35-Hosting-Integration.md`, "Stopping").
    pub fn with_stop(mut self, stop: CancellationToken) -> Self {
        self.stop = Some(stop);
        self
    }
}

/// A thread
/// [`CompiledGraph::recover_stale`](crate::graph::CompiledGraph::recover_stale)
/// recovered, and how its run ended. `at` is where it continued, one node per
/// level from the outermost graph in, and `recoveries` how many times it has
/// now been recovered without a node committing in between.
pub struct RecoveredThread<S> {
    pub thread_id: String,
    pub at: Vec<String>,
    pub recoveries: u32,
    pub result: GraphExecutionResult<S>,
}

/// What one
/// [`CompiledGraph::recover_stale`](crate::graph::CompiledGraph::recover_stale)
/// call did: the threads it recovered, each with its run's result, and the
/// `running` threads it left alone, each with the reason (a live lease holds
/// it, it isn't stale yet, another recovery got there first, …).
pub struct RecoveryReport<S> {
    pub recovered: Vec<RecoveredThread<S>>,
    pub skipped: Vec<SkippedThread>,
}
