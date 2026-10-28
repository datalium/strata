//! Types for `schema/checkpoint.schema.json` and `schema/graph-definition.schema.json`.
//!
//! These must stay in lockstep with the JSON Schema files and with the C#
//! equivalents in `Datalium.Strata.Storage/Schema`. See `schema/README.md`.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::parallel::{JoinKind, PartialResults};

pub const SCHEMA_VERSION: &str = "1.0.0";

/// The `schema_version` of a checkpoint that carries a [`Checkpoint::nested`]
/// child. A checkpoint without one is still written as [`SCHEMA_VERSION`], so
/// everything that existed before nesting is byte-for-byte unchanged, and a
/// reader that doesn't know about nesting can tell it is out of its depth.
pub const NESTED_CHECKPOINT_SCHEMA_VERSION: &str = "1.1.0";

/// The `schema_version` of a checkpoint that is [`CheckpointStatus::Cancelled`],
/// or nests a cancelled one at any depth (`design/17-Async-Execution.md`).
/// Everything else keeps the version it had before, so a reader that predates
/// cancellation can tell it is out of its depth.
pub const CANCELLED_CHECKPOINT_SCHEMA_VERSION: &str = "1.2.0";

/// The `schema_version` of a checkpoint that carries a [`Checkpoint::decision`]
/// at any depth (`design/21-Typed-Human-Signals.md`). A reader that predates
/// decisions would drop it and could resume a rejection down the approval
/// path, so it must refuse the checkpoint.
pub const DECISION_CHECKPOINT_SCHEMA_VERSION: &str = "1.3.0";

/// The `schema_version` of a [`CheckpointStatus::Ended`] checkpoint
/// (`design/28-Ending-A-Thread.md`), a superset of
/// [`DECISION_CHECKPOINT_SCHEMA_VERSION`]. A reader that predates it would see
/// a status it can't parse, so it must refuse the checkpoint.
pub const ENDED_CHECKPOINT_SCHEMA_VERSION: &str = "1.4.0";

/// The `schema_version` of a checkpoint a level of which carries a
/// [`Checkpoint::wait`] (`design/29-Event-Waits-And-Timers.md`), a superset of
/// [`ENDED_CHECKPOINT_SCHEMA_VERSION`]. A reader that predates it would resume
/// an event wait or a timer as if a human had answered it, so it must refuse
/// the checkpoint.
pub const WAIT_CHECKPOINT_SCHEMA_VERSION: &str = "1.5.0";

/// The `schema_version` of a checkpoint a level of which carries a
/// [`Checkpoint::graph`] (`design/31-Graph-Identity.md`), a superset of
/// [`WAIT_CHECKPOINT_SCHEMA_VERSION`]. A reader that predates it can't check
/// which graph wrote the thread and would resume it under any, so it must
/// refuse the checkpoint.
pub const GRAPH_CHECKPOINT_SCHEMA_VERSION: &str = "1.6.0";

/// The `schema_version` of a [`CheckpointStatus::Running`] checkpoint
/// (`design/33-Durable-Execution.md`), a superset of
/// [`GRAPH_CHECKPOINT_SCHEMA_VERSION`]. A reader that predates it would see a
/// status it can't parse, so it must refuse the checkpoint.
pub const RUNNING_CHECKPOINT_SCHEMA_VERSION: &str = "1.7.0";

/// Whether this build can read a checkpoint written with `version`.
pub fn is_supported_checkpoint_version(version: &str) -> bool {
    version == SCHEMA_VERSION
        || version == NESTED_CHECKPOINT_SCHEMA_VERSION
        || version == CANCELLED_CHECKPOINT_SCHEMA_VERSION
        || version == DECISION_CHECKPOINT_SCHEMA_VERSION
        || version == ENDED_CHECKPOINT_SCHEMA_VERSION
        || version == WAIT_CHECKPOINT_SCHEMA_VERSION
        || version == GRAPH_CHECKPOINT_SCHEMA_VERSION
        || version == RUNNING_CHECKPOINT_SCHEMA_VERSION
}

/// The lowest version that can express `checkpoint`:
/// [`RUNNING_CHECKPOINT_SCHEMA_VERSION`] if it is running, else
/// [`GRAPH_CHECKPOINT_SCHEMA_VERSION`] if a level of it carries a
/// [`Checkpoint::graph`], else
/// [`ENDED_CHECKPOINT_SCHEMA_VERSION`] if it is ended, else
/// [`WAIT_CHECKPOINT_SCHEMA_VERSION`] if a level of it waits with a
/// [`Checkpoint::wait`], else
/// [`DECISION_CHECKPOINT_SCHEMA_VERSION`] if anything in it carries a
/// decision, else [`CANCELLED_CHECKPOINT_SCHEMA_VERSION`] if anything in it is
/// cancelled, else [`NESTED_CHECKPOINT_SCHEMA_VERSION`] if it nests a child,
/// else [`SCHEMA_VERSION`].
pub(crate) fn checkpoint_version_of(checkpoint: &Checkpoint) -> &'static str {
    // Every level of a running checkpoint is running, so the outermost says it.
    if checkpoint.status == CheckpointStatus::Running {
        return RUNNING_CHECKPOINT_SCHEMA_VERSION;
    }
    let mut level = Some(checkpoint);
    while let Some(current) = level {
        if current.graph.is_some() {
            return GRAPH_CHECKPOINT_SCHEMA_VERSION;
        }
        level = current.nested.as_deref();
    }
    if checkpoint.status == CheckpointStatus::Ended {
        return ENDED_CHECKPOINT_SCHEMA_VERSION;
    }
    if checkpoint.innermost_wait().is_some() {
        return WAIT_CHECKPOINT_SCHEMA_VERSION;
    }
    let mut cancelled = false;
    let mut level = Some(checkpoint);
    while let Some(current) = level {
        if current.decision.is_some() {
            return DECISION_CHECKPOINT_SCHEMA_VERSION;
        }
        cancelled |= current.status == CheckpointStatus::Cancelled;
        level = current.nested.as_deref();
    }
    if cancelled {
        CANCELLED_CHECKPOINT_SCHEMA_VERSION
    } else if checkpoint.nested.is_some() {
        NESTED_CHECKPOINT_SCHEMA_VERSION
    } else {
        SCHEMA_VERSION
    }
}

/// `checkpoint` with the `schema_version` its content needs.
pub(crate) fn versioned(mut checkpoint: Checkpoint) -> Checkpoint {
    checkpoint.schema_version = checkpoint_version_of(&checkpoint).to_string();
    checkpoint
}

/// A [`crate::checkpoint::HumanSignal`] as the engine recorded it, when it
/// answered an interrupt boundary (`design/21-Typed-Human-Signals.md`); the
/// checkpoint's `decision` object. Every node of that graph level sees it
/// ([`crate::context::NodeContext::decision`]) until another boundary of the
/// level is answered, and every checkpoint the level writes meanwhile carries
/// it, so a failed thread resumed later still knows who approved. Resuming a
/// failed or cancelled thread doesn't replace it. Mirrors .NET's
/// `HumanDecision`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HumanDecision {
    /// The interrupt boundary it answered, a node of its own level.
    pub boundary: String,
    /// The signal's `is_approved`.
    pub is_approved: bool,
    /// The signal's `reviewer_token`: who answered.
    pub reviewer: String,
    /// Every override the signal applied, sorted by key: typed ones with
    /// their JSON type, string ones as JSON strings.
    pub overrides: serde_json::Map<String, Value>,
    /// When the resume that answered began, as an RFC 3339 string.
    pub decided_at: String,
}

/// What a waiting thread waits for (`design/29-Event-Waits-And-Timers.md`).
/// Mirrors .NET's `WaitKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitKind {
    /// A human, at an interrupt boundary with a timeout: `resume` answers it.
    Human,
    /// An external event, at a wait node: `signal` with its event type
    /// answers it.
    Event,
    /// A time, at a timer: `wake_due` wakes it once it is due.
    Timer,
}

impl WaitKind {
    /// The wire name: `human`, `event` or `timer`.
    pub fn as_str(self) -> &'static str {
        match self {
            WaitKind::Human => "human",
            WaitKind::Event => "event",
            WaitKind::Timer => "timer",
        }
    }
}

/// Why a checkpoint level waits: its `wait` object
/// (`design/29-Event-Waits-And-Timers.md`). `event_type` is set exactly for
/// [`WaitKind::Event`]. `wake_at` is when a timer fires or a wait times out,
/// as an RFC 3339 string at whole seconds; `None` for an event wait with no
/// timeout, which is never due. Every key is always written. Mirrors .NET's
/// `CheckpointWait`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointWait {
    pub kind: WaitKind,
    #[serde(default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub wake_at: Option<String>,
}

impl CheckpointWait {
    /// What it waits for, for messages: `event 'x'`, `a timer`, `a human`.
    pub(crate) fn describe(&self) -> String {
        match self.kind {
            WaitKind::Event => {
                format!("event '{}'", self.event_type.as_deref().unwrap_or_default())
            }
            WaitKind::Timer => "a timer".to_string(),
            WaitKind::Human => "a human".to_string(),
        }
    }
}

/// Which graph wrote a checkpoint level (`design/31-Graph-Identity.md`): the
/// id and optional version a graph was compiled with
/// ([`crate::graph::CompileOptions::with_graph_id`] /
/// [`crate::graph::CompileOptions::with_graph_version`]). Both are free
/// text, compared exactly: versions have no order. The checkpoint's `graph`
/// object, and a definition's `identity`; both keys are always written.
/// Mirrors .NET's `GraphIdentity`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GraphIdentity {
    pub id: String,
    #[serde(default)]
    pub version: Option<String>,
}

impl GraphIdentity {
    pub fn new(id: impl Into<String>, version: Option<String>) -> Self {
        Self {
            id: id.into(),
            version,
        }
    }

    /// For messages: `version 2`, or `unversioned`.
    pub(crate) fn version_text(version: Option<&str>) -> String {
        version.map_or_else(|| "unversioned".to_string(), |v| format!("version {v}"))
    }
}

impl std::fmt::Display for GraphIdentity {
    /// For messages: `'incident' version 2`, or `'incident' unversioned`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "'{}' {}",
            self.id,
            Self::version_text(self.version.as_deref())
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStatus {
    Interrupted,
    Completed,
    Failed,
    /// The run was cancelled, or its deadline passed, while a node was running
    /// or about to start (`design/17-Async-Execution.md`). Resumable once,
    /// like `Failed`: a resume re-enters that node with the last committed
    /// state.
    Cancelled,
    /// The thread was ended where it stood, by
    /// [`crate::graph::CompiledGraph::end`] (`design/28-Ending-A-Thread.md`).
    /// Terminal: a resume is refused, and ending it again changes nothing.
    /// Every level of the checkpoint is ended, and none has a next node.
    Ended,
    /// A run is going on, or its process died
    /// (`design/33-Durable-Execution.md`): what a graph compiled with
    /// [`CheckpointPolicy::EveryNode`](crate::graph::CheckpointPolicy::EveryNode)
    /// writes before its first node and after every node that commits, and
    /// what a resume of one claims its thread with. Not resumable: a run that
    /// died is recovered ([`crate::graph::CompiledGraph::recover`]). Every
    /// level of the checkpoint is running, and each has a next node.
    Running,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub schema_version: String,
    pub thread_id: String,
    pub status: CheckpointStatus,
    #[serde(default)]
    pub suspended_node: Option<String>,
    #[serde(default)]
    pub next_node: Option<String>,
    pub frozen_state: Value,
    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>,
    pub created_at: String,
    #[serde(default)]
    pub resumed_at: Option<String>,
    /// The checkpoint of a child graph that is itself interrupted (or, on a
    /// `Failed` or `Cancelled` checkpoint, that failed or was cancelled), when
    /// this checkpoint's `suspended_node` is a subgraph node
    /// (`design/16-Nested-Interrupts.md`). Its `thread_id` is the name of that
    /// subgraph node, its status is its parent's — except that a `Failed`
    /// parent may nest a `Cancelled` child, stopped by the parent node's
    /// timeout (`design/17-Async-Execution.md`) — and it may nest further.
    /// Only present on an interrupted, failed or cancelled checkpoint, which is
    /// then written as [`NESTED_CHECKPOINT_SCHEMA_VERSION`], or as
    /// [`CANCELLED_CHECKPOINT_SCHEMA_VERSION`] if anything in it is cancelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<Box<Checkpoint>>,
    /// The decision in effect at this checkpoint's level when it was written:
    /// the answer to the level's most recent interrupt boundary
    /// (`design/21-Typed-Human-Signals.md`), which a resume restores. `None`
    /// when no boundary of the level has been answered. A checkpoint carrying
    /// one at any depth is written as [`DECISION_CHECKPOINT_SCHEMA_VERSION`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<HumanDecision>,
    /// Why this level waits, when it waits for more than a human with no
    /// timeout (`design/29-Event-Waits-And-Timers.md`): an event, a timer, or a
    /// human with a timeout, and when it wakes or times out. Only an
    /// interrupted level carries one, and only the innermost level of a
    /// checkpoint can. A checkpoint carrying one is written as
    /// [`WAIT_CHECKPOINT_SCHEMA_VERSION`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<CheckpointWait>,
    /// The graph that wrote this level (`design/31-Graph-Identity.md`): the
    /// identity it was compiled with, or `None` when it had none (or the
    /// checkpoint predates identities), which any graph may resume. A nested
    /// level carries its own subgraph's. A checkpoint carrying one at any
    /// level is written as [`GRAPH_CHECKPOINT_SCHEMA_VERSION`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<GraphIdentity>,
}

impl Checkpoint {
    /// The [`Checkpoint::wait`] of the innermost level, the only one that can
    /// have one.
    pub fn innermost_wait(&self) -> Option<&CheckpointWait> {
        let mut level = self;
        while let Some(child) = level.nested.as_deref() {
            level = child;
        }
        level.wait.as_ref()
    }

    /// Where this thread is waiting (or, for a failed one, stopped): the
    /// `suspended_node` of every level, from the outermost graph in. `["Work",
    /// "Approve"]` for a boundary at `Approve` inside the subgraph node `Work`;
    /// just `["Approve"]` when nothing is nested. Empty if the checkpoint has no
    /// `suspended_node`.
    pub fn waiting_at(&self) -> Vec<String> {
        let mut path: Vec<String> = self.suspended_node.iter().cloned().collect();
        let mut level = self.nested.as_deref();
        while let Some(child) = level {
            path.extend(child.suspended_node.iter().cloned());
            level = child.nested.as_deref();
        }
        path
    }
}

/// The `schema_version` of a graph definition with a parallel group or
/// subgraph node at its own level (`design/22-Graph-Structure-In-Definitions.md`).
/// Every other definition is still written as [`SCHEMA_VERSION`], byte for
/// byte as before, and a nested [`GraphNodeDef::graph`] computes its own.
pub const COMPOSITE_GRAPH_SCHEMA_VERSION: &str = "1.1.0";

/// The `schema_version` of a graph definition with a parallel group at its own
/// level whose join isn't the default
/// (`design/26-Parallel-Group-Controls-And-Joins.md`).
pub const JOIN_GRAPH_SCHEMA_VERSION: &str = "1.2.0";

/// The `schema_version` of a graph definition with a map at its own level
/// (`design/27-Map-Fan-Out.md`). It is a superset of
/// [`JOIN_GRAPH_SCHEMA_VERSION`]: a group at that level may have a join too.
pub const MAP_GRAPH_SCHEMA_VERSION: &str = "1.3.0";

/// The `schema_version` of a graph definition with a wait node at its own
/// level (`design/29-Event-Waits-And-Timers.md`). It is a superset of
/// [`MAP_GRAPH_SCHEMA_VERSION`].
pub const WAIT_GRAPH_SCHEMA_VERSION: &str = "1.4.0";

/// The `schema_version` of a graph definition whose level has an
/// [`GraphDefinition::identity`] (`design/31-Graph-Identity.md`). It is a
/// superset of [`WAIT_GRAPH_SCHEMA_VERSION`].
pub const IDENTITY_GRAPH_SCHEMA_VERSION: &str = "1.5.0";

/// The `schema_version` of a graph definition with an interrupt boundary at
/// its own level, a node with an [`GraphNodeDef::interrupt`]
/// (`design/48-Boundaries-In-Definitions.md`). It is a superset of
/// [`IDENTITY_GRAPH_SCHEMA_VERSION`].
pub const BOUNDARY_GRAPH_SCHEMA_VERSION: &str = "1.6.0";

/// Whether this build can read a graph definition written with `version`.
pub fn is_supported_graph_version(version: &str) -> bool {
    version == SCHEMA_VERSION
        || version == COMPOSITE_GRAPH_SCHEMA_VERSION
        || version == JOIN_GRAPH_SCHEMA_VERSION
        || version == MAP_GRAPH_SCHEMA_VERSION
        || version == WAIT_GRAPH_SCHEMA_VERSION
        || version == IDENTITY_GRAPH_SCHEMA_VERSION
        || version == BOUNDARY_GRAPH_SCHEMA_VERSION
}

/// The lowest version that can express a level with this `identity` and
/// these `nodes`: [`BOUNDARY_GRAPH_SCHEMA_VERSION`] if one of them is an
/// interrupt boundary, [`IDENTITY_GRAPH_SCHEMA_VERSION`] if it has an identity,
/// [`WAIT_GRAPH_SCHEMA_VERSION`] if one of them is a wait node,
/// [`MAP_GRAPH_SCHEMA_VERSION`] if one of them is a map,
/// [`JOIN_GRAPH_SCHEMA_VERSION`] if a group among them has a `join`,
/// [`COMPOSITE_GRAPH_SCHEMA_VERSION`] if one of them has a `kind`, else
/// [`SCHEMA_VERSION`].
pub(crate) fn graph_version_of(
    identity: Option<&GraphIdentity>,
    nodes: &[GraphNodeDef],
) -> &'static str {
    if nodes.iter().any(|node| node.interrupt.is_some()) {
        BOUNDARY_GRAPH_SCHEMA_VERSION
    } else if identity.is_some() {
        IDENTITY_GRAPH_SCHEMA_VERSION
    } else if nodes
        .iter()
        .any(|node| node.kind == Some(GraphNodeKind::Wait))
    {
        WAIT_GRAPH_SCHEMA_VERSION
    } else if nodes
        .iter()
        .any(|node| node.kind == Some(GraphNodeKind::Map))
    {
        MAP_GRAPH_SCHEMA_VERSION
    } else if nodes.iter().any(|node| node.join.is_some()) {
        JOIN_GRAPH_SCHEMA_VERSION
    } else if nodes.iter().any(|node| node.kind.is_some()) {
        COMPOSITE_GRAPH_SCHEMA_VERSION
    } else {
        SCHEMA_VERSION
    }
}

/// A parallel group's join as the definition exports it
/// (`design/26-Parallel-Group-Controls-And-Joins.md`): the policy, a quorum's
/// `count`, a `first_matching` predicate's description as `condition`, and
/// what an unmet join does. The predicate itself isn't exported. Every key is
/// always written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphJoinDef {
    pub policy: JoinKind,
    pub count: Option<usize>,
    pub condition: Option<String>,
    pub partial: PartialResults,
}

/// What a composite node is (`design/22-Graph-Structure-In-Definitions.md`).
/// A plain node has no kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphNodeKind {
    /// A parallel group: [`GraphNodeDef::branches`] holds its branches.
    ParallelGroup,
    /// A subgraph: [`GraphNodeDef::graph`] holds the child's whole definition.
    Subgraph,
    /// A map (`design/27-Map-Fan-Out.md`): [`GraphNodeDef::graph`] holds the
    /// definition of the child each item runs, its template.
    Map,
    /// A wait node (`design/29-Event-Waits-And-Timers.md`):
    /// [`GraphNodeDef::wait`] says what it waits for.
    Wait,
}

/// A wait node as the definition exports it
/// (`design/29-Event-Waits-And-Timers.md`): what it waits for
/// ([`WaitKind::Event`] or [`WaitKind::Timer`]), an event's type, a timer's
/// fixed delay (`None` when it is computed from the state), and an event
/// wait's timeout, in milliseconds. Every key is always written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphWaitDef {
    pub kind: WaitKind,
    pub event_type: Option<String>,
    pub delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
}

/// An interrupt boundary as the definition exports it
/// (`design/48-Boundaries-In-Definitions.md`): the run freezes after the node
/// until a human answers it, and with a `timeout_ms` (whole milliseconds) it
/// times out, continuing at its guarded `on_timeout` edge or ending the thread
/// (`design/29-Event-Waits-And-Timers.md`). Its routes are edges, not repeated
/// here. The key is always written.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GraphInterruptDef {
    pub timeout_ms: Option<u64>,
}

/// One branch of a parallel group, in declaration order. Branches have no
/// edges: they all start when the group starts.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GraphBranchDef {
    pub name: String,
    #[serde(default)]
    pub metadata: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct GraphNodeDef {
    pub name: String,
    /// Description only, set with
    /// [`crate::graph::StateGraph::set_node_metadata`]; the engine never reads
    /// it. A `BTreeMap`, so the same graph exports the same bytes every time.
    #[serde(default)]
    pub metadata: Option<BTreeMap<String, String>>,
    /// Absent for a plain node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<GraphNodeKind>,
    /// Present exactly when `kind` is [`GraphNodeKind::ParallelGroup`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branches: Option<Vec<GraphBranchDef>>,
    /// Present exactly when `kind` is [`GraphNodeKind::Subgraph`] or
    /// [`GraphNodeKind::Map`]: the child's (or the map's template's) whole
    /// definition, with its own `schema_version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<Box<GraphDefinition>>,
    /// A parallel group's or a map's join, when it isn't the default
    /// (`design/26-Parallel-Group-Controls-And-Joins.md`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<GraphJoinDef>,
    /// A wait node's wait (`design/29-Event-Waits-And-Timers.md`): present
    /// exactly when `kind` is [`GraphNodeKind::Wait`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<GraphWaitDef>,
    /// Present exactly when the node is an interrupt boundary
    /// (`design/48-Boundaries-In-Definitions.md`), of any kind but a wait.
    /// A level with one is [`BOUNDARY_GRAPH_SCHEMA_VERSION`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupt: Option<GraphInterruptDef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphEdgeDef {
    pub from: String,
    pub to: String,
    pub guarded: bool,
    #[serde(default)]
    pub condition: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphDefinition {
    pub schema_version: String,
    /// The identity the graph was compiled with
    /// (`design/31-Graph-Identity.md`): absent for a builder's definition, or
    /// a graph without one. A level with one is
    /// [`IDENTITY_GRAPH_SCHEMA_VERSION`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<GraphIdentity>,
    pub entry_point: String,
    pub nodes: Vec<GraphNodeDef>,
    pub edges: Vec<GraphEdgeDef>,
}

impl GraphDefinition {
    /// Checks what a file read from disk must satisfy before it is validated
    /// as a graph (`design/22-Graph-Structure-In-Definitions.md`), at every
    /// level: a `schema_version` this build knows, no `kind` in a `1.0.0`
    /// level, a `map` only at `1.3.0` or later, a `wait` node only at `1.4.0`
    /// or later, an `identity` only at `1.5.0` or later and with a non-empty
    /// id, an `interrupt` only at `1.6.0` and never on a wait node, every
    /// node's `kind` matching its fields (a group has `branches` and no
    /// `graph`, a subgraph or a map a `graph` and no `branches`, a wait node a
    /// `wait` and neither, a plain node none of them), and a `join` only on a
    /// group or a map, at `1.2.0` or later, with only the fields its policy
    /// has; and at a `1.6.0` level, an `on_reject` edge only from an
    /// interrupt boundary and an `on_timeout` edge only from a node with a
    /// timeout (`design/48-Boundaries-In-Definitions.md`). The engines never
    /// write a definition that fails this, so a failure means a broken file, not a broken graph: `strata
    /// graph validate` refuses it with exit `2`. The error names the node by
    /// its qualified path.
    pub fn check_shape(&self) -> Result<(), String> {
        self.check_shape_at("")
    }

    fn check_shape_at(&self, prefix: &str) -> Result<(), String> {
        let at = if prefix.is_empty() {
            String::new()
        } else {
            format!(" (in '{}')", prefix.trim_end_matches('/'))
        };
        let Some(rank) = graph_version_rank(&self.schema_version) else {
            return Err(format!(
                "unsupported schema_version '{}'{at}: this build reads {SCHEMA_VERSION}, {COMPOSITE_GRAPH_SCHEMA_VERSION}, {JOIN_GRAPH_SCHEMA_VERSION}, {MAP_GRAPH_SCHEMA_VERSION}, {WAIT_GRAPH_SCHEMA_VERSION}, {IDENTITY_GRAPH_SCHEMA_VERSION} and {BOUNDARY_GRAPH_SCHEMA_VERSION}",
                self.schema_version
            ));
        };
        // Each version is a superset of every earlier one
        // (design/31-Graph-Identity.md, design/48-Boundaries-In-Definitions.md).
        let has = |version: &str| graph_version_rank(version).is_some_and(|min| rank >= min);
        if let Some(identity) = &self.identity {
            if !has(IDENTITY_GRAPH_SCHEMA_VERSION) {
                return Err(format!(
                    "the definition{at} has an identity, which schema_version {} doesn't have",
                    self.schema_version
                ));
            }
            if identity.id.trim().is_empty() {
                return Err(format!(
                    "the definition{at} has an identity with an empty id"
                ));
            }
        }
        for node in &self.nodes {
            let path = format!("{prefix}{}", node.name);
            if node.kind.is_some() && !has(COMPOSITE_GRAPH_SCHEMA_VERSION) {
                return Err(format!(
                    "node '{path}' has a kind, which schema_version {SCHEMA_VERSION} doesn't have"
                ));
            }
            if node.kind == Some(GraphNodeKind::Map) && !has(MAP_GRAPH_SCHEMA_VERSION) {
                return Err(format!(
                    "node '{path}' is a map, which schema_version {} doesn't have",
                    self.schema_version
                ));
            }
            if node.kind == Some(GraphNodeKind::Wait) && !has(WAIT_GRAPH_SCHEMA_VERSION) {
                return Err(format!(
                    "node '{path}' is a wait, which schema_version {} doesn't have",
                    self.schema_version
                ));
            }
            match (node.kind == Some(GraphNodeKind::Wait), &node.wait) {
                (true, None) => return Err(format!("node '{path}' is a wait but has no wait")),
                (false, Some(_)) => {
                    return Err(format!("node '{path}' has a wait but is not a wait node"));
                }
                (true, Some(wait)) if wait.kind == WaitKind::Human => {
                    return Err(format!(
                        "node '{path}' has a wait of kind 'human', which is neither event nor timer"
                    ));
                }
                _ => {}
            }
            if node.interrupt.is_some() {
                if !has(BOUNDARY_GRAPH_SCHEMA_VERSION) {
                    return Err(format!(
                        "node '{path}' has an interrupt, which schema_version {} doesn't have",
                        self.schema_version
                    ));
                }
                if node.kind == Some(GraphNodeKind::Wait) {
                    return Err(format!(
                        "node '{path}' is a wait and can't also have an interrupt"
                    ));
                }
            }
            let (branches, graph) = (node.branches.is_some(), node.graph.is_some());
            let fits = match node.kind {
                None | Some(GraphNodeKind::Wait) => !branches && !graph,
                Some(GraphNodeKind::ParallelGroup) => branches && !graph,
                Some(GraphNodeKind::Subgraph | GraphNodeKind::Map) => graph && !branches,
            };
            if !fits {
                let kind = match node.kind {
                    None => "a plain node",
                    Some(GraphNodeKind::ParallelGroup) => "a parallel_group",
                    Some(GraphNodeKind::Subgraph) => "a subgraph",
                    Some(GraphNodeKind::Map) => "a map",
                    Some(GraphNodeKind::Wait) => "a wait",
                };
                let fields = match (branches, graph) {
                    (false, false) => "neither branches nor graph",
                    (true, false) => "branches",
                    (false, true) => "graph",
                    (true, true) => "both branches and graph",
                };
                return Err(format!("node '{path}' is {kind} but has {fields}"));
            }
            if let Some(join) = &node.join {
                if !has(JOIN_GRAPH_SCHEMA_VERSION) {
                    return Err(format!(
                        "node '{path}' has a join, which schema_version {} doesn't have",
                        self.schema_version
                    ));
                }
                if !matches!(
                    node.kind,
                    Some(GraphNodeKind::ParallelGroup | GraphNodeKind::Map)
                ) {
                    return Err(format!(
                        "node '{path}' has a join but is neither a parallel_group nor a map"
                    ));
                }
                if join.count.is_some() && join.policy != JoinKind::Quorum {
                    return Err(format!(
                        "node '{path}' has a join with a count, which only a quorum has"
                    ));
                }
                if join.condition.is_some() && join.policy != JoinKind::FirstMatching {
                    return Err(format!(
                        "node '{path}' has a join with a condition, which only first_matching has"
                    ));
                }
            }
            if let Some(child) = &node.graph {
                child.check_shape_at(&format!("{path}/"))?;
            }
        }
        if has(BOUNDARY_GRAPH_SCHEMA_VERSION) {
            self.check_routes(prefix)?;
        }
        Ok(())
    }

    /// At a `1.6.0` level, where a boundary says it is one
    /// (`design/48-Boundaries-In-Definitions.md`): an `on_reject` edge comes
    /// from an interrupt boundary, and an `on_timeout` edge from a node with a
    /// timeout (a boundary's or an event wait's). An edge from a name that
    /// isn't a node is the validator's (a dangling edge). Before `1.6.0` no
    /// boundary was marked, so these aren't checked there.
    fn check_routes(&self, prefix: &str) -> Result<(), String> {
        for edge in &self.edges {
            let Some(node) = self.nodes.iter().find(|node| node.name == edge.from) else {
                continue;
            };
            let path = format!("{prefix}{}", node.name);
            match edge.condition.as_deref() {
                Some("on_reject") if node.interrupt.is_none() => {
                    return Err(format!(
                        "node '{path}' has an on_reject edge but is not an interrupt boundary"
                    ));
                }
                Some("on_timeout") => {
                    let timed = node
                        .interrupt
                        .as_ref()
                        .is_some_and(|interrupt| interrupt.timeout_ms.is_some())
                        || node
                            .wait
                            .as_ref()
                            .is_some_and(|wait| wait.timeout_ms.is_some());
                    if !timed {
                        return Err(format!(
                            "node '{path}' has an on_timeout edge but no timeout"
                        ));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Where `version` stands among the graph definition versions this build
/// reads, oldest first; `None` for one it doesn't know.
fn graph_version_rank(version: &str) -> Option<usize> {
    [
        SCHEMA_VERSION,
        COMPOSITE_GRAPH_SCHEMA_VERSION,
        JOIN_GRAPH_SCHEMA_VERSION,
        MAP_GRAPH_SCHEMA_VERSION,
        WAIT_GRAPH_SCHEMA_VERSION,
        IDENTITY_GRAPH_SCHEMA_VERSION,
        BOUNDARY_GRAPH_SCHEMA_VERSION,
    ]
    .iter()
    .position(|known| *known == version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture(name: &str) -> String {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e))
    }

    #[test]
    fn checkpoint_round_trips_through_the_shared_fixture() {
        let raw = fixture("checkpoint.sample.json");
        let original: Value = serde_json::from_str(&raw).unwrap();

        let checkpoint: Checkpoint = serde_json::from_str(&raw).unwrap();
        let round_tripped = serde_json::to_value(&checkpoint).unwrap();

        assert_eq!(original, round_tripped);
    }

    #[test]
    fn graph_definition_round_trips_through_the_shared_fixture() {
        let raw = fixture("graph-definition.sample.json");
        let original: Value = serde_json::from_str(&raw).unwrap();

        let graph: GraphDefinition = serde_json::from_str(&raw).unwrap();
        let round_tripped = serde_json::to_value(&graph).unwrap();

        assert_eq!(original, round_tripped);
    }

    #[test]
    fn the_hierarchy_fixture_round_trips_and_has_a_sound_shape() {
        let raw = fixture("graph-export/hierarchy.json");
        let original: Value = serde_json::from_str(&raw).unwrap();

        let graph: GraphDefinition = serde_json::from_str(&raw).unwrap();
        assert_eq!(original, serde_json::to_value(&graph).unwrap());
        assert_eq!(graph.check_shape(), Ok(()));
    }

    fn hierarchy() -> Value {
        serde_json::from_str(&fixture("graph-export/hierarchy.json")).unwrap()
    }

    fn shape_of(value: Value) -> Result<(), String> {
        serde_json::from_value::<GraphDefinition>(value)
            .unwrap()
            .check_shape()
    }

    #[test]
    fn every_level_must_have_a_known_version() {
        let mut top = hierarchy();
        top["schema_version"] = "2.0.0".into();
        assert_eq!(
            shape_of(top),
            Err(
                "unsupported schema_version '2.0.0': this build reads 1.0.0, 1.1.0, 1.2.0, 1.3.0, 1.4.0, 1.5.0 and 1.6.0"
                    .into()
            )
        );

        let mut nested = hierarchy();
        nested["nodes"][2]["graph"]["nodes"][2]["graph"]["schema_version"] = "1.9.0".into();
        assert_eq!(
            shape_of(nested),
            Err(
                "unsupported schema_version '1.9.0' (in 'Remediate/Fix'): this build reads 1.0.0, 1.1.0, 1.2.0, 1.3.0, 1.4.0, 1.5.0 and 1.6.0"
                    .into()
            )
        );
    }

    #[test]
    fn a_kind_needs_1_1_0_and_its_own_field() {
        let mut old = hierarchy();
        old["schema_version"] = "1.0.0".into();
        assert_eq!(
            shape_of(old),
            Err("node 'Investigate' has a kind, which schema_version 1.0.0 doesn't have".into())
        );

        let mut no_graph = hierarchy();
        no_graph["nodes"][2]["graph"]["nodes"][2]
            .as_object_mut()
            .unwrap()
            .remove("graph");
        assert_eq!(
            shape_of(no_graph),
            Err("node 'Remediate/Fix' is a subgraph but has neither branches nor graph".into())
        );

        let mut plain_with_branches = hierarchy();
        plain_with_branches["nodes"][0]["branches"] = serde_json::json!([]);
        assert_eq!(
            shape_of(plain_with_branches),
            Err("node 'Triage' is a plain node but has branches".into())
        );

        let mut group_with_graph = hierarchy();
        group_with_graph["nodes"][1]["graph"] = hierarchy()["nodes"][2]["graph"].clone();
        assert_eq!(
            shape_of(group_with_graph),
            Err("node 'Investigate' is a parallel_group but has both branches and graph".into())
        );
    }

    #[test]
    fn an_unknown_kind_is_not_a_graph_definition() {
        let mut unknown = hierarchy();
        unknown["nodes"][1]["kind"] = "loop".into();
        assert!(serde_json::from_value::<GraphDefinition>(unknown).is_err());
    }
}
