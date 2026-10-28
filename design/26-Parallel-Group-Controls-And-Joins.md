# Datalium Strata: Parallel Group Controls and Join Policies

`design/14-Parallel-Execution.md` built the parallel group: named branches run concurrently, merged by a reducer in declaration order. It listed as not built:

- **A concurrency limit.** Every branch starts at once.
- **Timeouts per branch.** `SetTimeout` on the group covers the whole group.
- **Opting out of sibling cancellation**, and **partial results.** The first branch to fail fails the group and cancels the rest; the reducer never sees what the others found.
- **Any join but "all".** A group always waits for every branch.

StrataOps needs all of them for its M2 (real models):

- **SG-07, the specialists group.** `investigate` runs five specialists (telemetry, kubernetes, deployment, database, dependency). A hung Kubernetes API must not stall the investigation, so each specialist needs its own timeout (45 s), and four sets of evidence still make an RCA, so the reducer must see every branch's result. A concurrency limit keeps it under the Model Fabric's budget. The workaround, catching inside each branch, hides the failures from the run's events.
- **SG-08, the `rca-race` subgraph.** Three branches (`openai`, `anthropic`, `local`) run the RCA agent on different models. `FIRST_CONFIDENT` takes the first answer that passes the deterministic validators with confidence ≥ 0.9 and cancels the rest; when none is confident, it wants the best of all of them. The live graph must show the winner and the cancelled losers, from run events.

This document specifies **SG-07** and **SG-08** together, because they are one mechanism: an options type for the group, a result type for its branches, and a join policy that decides when the group is done. `design/27` builds the map (SG-06) on the same runner, options and results. The SG numbers refer to the gap log of StrataOps (not public yet).

Both land in both engines at one commit (strict parity, StrataOps ADR 0003): the same capability, in each language's idiom.

It was written as a blueprint before implementation and is now built. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The options | **`ParallelGroupOptions<TState>`** in both engines (.NET `init` properties, a Rust builder): `MaxConcurrency`, `BranchTimeout`, `CancelSiblingsOnFailure` (default true), `Partial` (`FailGroup`, the default, or `KeepSucceeded`) and `Join` (`All`, the default, `FirstSuccess`, `FirstMatching(predicate)` or `Quorum(n)`). A group without options behaves exactly as before. |
| When the group is done | **When its join is met** (All: every branch succeeded; FirstSuccess: one did; FirstMatching: one whose result passes the predicate did; Quorum(n): n did), or **when it can no longer be met**. Branches still running when the join is met are cancelled: they **lost the race**. |
| A failure | **Fails the group only once the join can't be met.** For All that is the first failure, as today; a race tolerates failures while another branch could still win. |
| `Partial` | What an unmet join does. **`FailGroup`** (default): the group fails, at once, cancelling the rest unless `CancelSiblingsOnFailure` is false. **`KeepSucceeded`**: the group waits for every branch and reduces over all of them, flagged as unmet; it fails only if **no** branch succeeded. |
| FirstMatching with no match | **The group fails under `FailGroup`; under `KeepSucceeded` the reducer gets every outcome, flagged unmet.** StrataOps' "best of all when none is confident" is `FirstMatching` + `KeepSucceeded`. |
| Branch results | A second reducer, **`AddParallelGroupWithOutcomes` / `add_parallel_group_with_outcomes`**, takes **`BranchOutcomes<TState>`**: every branch's `BranchOutcome` (name, status `Succeeded` / `Failed` / `TimedOut` / `Cancelled`, state when it succeeded, error), in declaration order, plus whether the join was met and which branches decided it. Today's reducer is unchanged and, with options, gets the states that count. |
| Per-branch timeout | **A transient failure of the branch** (status `TimedOut`), like a node timeout (`design/17`), on the run's clock (`design/25`). It starts when the branch starts, so a branch queued by the concurrency limit isn't charged for the wait. |
| Concurrency limit | At most `MaxConcurrency` branches run at once; the rest **start in declaration order** as slots free. It counts the branches the group is waiting for: a branch it gives up on frees its slot at once, even if its code is still unwinding (see "Timeouts and the concurrency limit"). |
| Group failure kind | **Transient if retrying could meet the join**, counting transient failures as possible successes: All fails transiently only if no failure was fatal (today's rule); a race fails transiently if any branch failed transiently. The failure carries the longest retry hint among them (`design/25`). |
| Per-branch retry | **Not built.** The group's policy retries the whole group, which re-runs every branch, as today. |
| Run events | `BranchCompleted` keeps its three results and gains a **`Reason`**: `TimedOut` (on `failed`), `LostRace` or `SiblingFailed` (on `cancelled`). A new **`JoinDecided { Policy, Met, Deciding }`** event, for a group whose join isn't All, says who won. |
| Export | **The join is exported** (it changes what the graph means): a `join` object on the group (`policy`, `count`, `condition`, `partial`) whenever it isn't the default. A level with one is **`schema_version` `1.2.0`**. Limits and timeouts are runtime tuning and aren't exported. |
| Validation | `STRATA_ERR_INVALID_QUORUM` (a quorum below 1 or above the branch count), in the shared validator, so `.compile()` and `strata graph validate` both refuse it. |
| Hops (SG-05) | Still **one hop** per group, whatever its options. |

---

## Usage

```csharp
// SG-07: five specialists, 45 s each, three at a time, keep what succeeded.
var specialists = new ParallelBranches<Investigation>()
    .Branch("telemetry", Telemetry).Branch("kubernetes", Kubernetes).Branch("deployment", Deployment)
    .Branch("database", Database).Branch("dependency", Dependency);

graph.AddParallelGroupWithOutcomes(
    "investigate",
    specialists,
    (entry, results) => entry with
    {
        Evidence = [.. results.Where(r => r.Status == BranchStatus.Succeeded).SelectMany(r => r.State!.Evidence)],
        Gaps = [.. results.Where(r => r.Status != BranchStatus.Succeeded).Select(r => $"{r.Name}: {r.Status}")],
    },
    new ParallelGroupOptions<Investigation>
    {
        BranchTimeout = TimeSpan.FromSeconds(45),
        MaxConcurrency = 3,
        Partial = PartialResults.KeepSucceeded,
    });

// SG-08: FIRST_CONFIDENT, falling back to the best of all.
rcaRace.AddParallelGroupWithOutcomes(
    "race",
    new ParallelBranches<Rca>().Branch("openai", OpenAi).Branch("anthropic", Anthropic).Branch("local", Local),
    (entry, results) => results.JoinMet
        ? results.Single(r => r.Name == results.Deciding[0]).State!
        : results.Succeeded.MaxBy(r => r.Confidence) ?? entry,
    new ParallelGroupOptions<Rca>
    {
        Join = JoinPolicy<Rca>.FirstMatching(r => r.Validated && r.Confidence >= 0.9, "validated, confidence >= 0.9"),
        Partial = PartialResults.KeepSucceeded,
    });
```

```rust
graph.add_parallel_group_with_outcomes(
    "investigate",
    specialists,
    |entry: &Investigation, results: BranchOutcomes<Investigation>| merge(entry, results),
    ParallelGroupOptions::new()
        .with_branch_timeout(Duration::from_secs(45))
        .with_max_concurrency(3)
        .keep_succeeded(),
);

rca_race.add_parallel_group_with_outcomes(
    "race",
    ParallelBranches::new().async_branch("openai", openai).async_branch("anthropic", anthropic).async_branch("local", local),
    |entry: &Rca, results: BranchOutcomes<Rca>| pick(entry, results),
    ParallelGroupOptions::new()
        .with_join(Join::first_matching(|r: &Rca| r.validated && r.confidence >= 0.9)
            .described("validated, confidence >= 0.9"))
        .keep_succeeded(),
);
```

`MAJORITY` and `CONSENSUS` stay as `All` (or `Quorum(n)`) plus a StrataOps reducer.

---

## The API

### .NET

```csharp
namespace Datalium.Strata.Graph;

/// How a parallel group runs its branches and decides it is done. A group added without options
/// behaves as one with the defaults below, which is how groups behaved before.
public sealed class ParallelGroupOptions<TState>
{
    /// At most this many branches run at once; the rest start in declaration order. Null: all at once.
    /// At least 1 (ArgumentOutOfRangeException from the setter).
    public int? MaxConcurrency { get; init; }

    /// Each branch's own timeout, from when it starts, on the run's clock: a transient failure of the
    /// branch. Null: none. Positive.
    public TimeSpan? BranchTimeout { get; init; }

    /// When the join can no longer be met under FailGroup, cancel the branches still running (the
    /// default) or let them finish first.
    public bool CancelSiblingsOnFailure { get; init; } = true;

    public PartialResults Partial { get; init; } = PartialResults.FailGroup;

    public JoinPolicy<TState> Join { get; init; } = JoinPolicy<TState>.All;
}

public enum PartialResults
{
    /// An unmet join fails the group.
    FailGroup,
    /// An unmet join reduces over every branch, flagged unmet; only "nothing succeeded" fails the group.
    KeepSucceeded,
}

/// When a parallel group is done.
public sealed class JoinPolicy<TState>
{
    public static JoinPolicy<TState> All { get; }
    public static JoinPolicy<TState> FirstSuccess { get; }
    /// The first branch whose result passes predicate. description is exported as the join's condition.
    public static JoinPolicy<TState> FirstMatching(Func<TState, bool> predicate, string? description = null);
    /// The first count branches to succeed (validated: 1 ≤ count ≤ branches).
    public static JoinPolicy<TState> Quorum(int count);

    public JoinKind Kind { get; }
    public int? Count { get; }
    public string? Description { get; }
}

public enum JoinKind { All, FirstSuccess, FirstMatching, Quorum }

public enum BranchStatus { Succeeded, Failed, TimedOut, Cancelled }

/// One branch's result, as an outcomes reducer sees it.
public sealed record BranchOutcome<TState>
{
    public required string Name { get; init; }
    public required BranchStatus Status { get; init; }
    public TState? State { get; init; }            // set exactly when Succeeded
    public NodeException? Error { get; init; }     // set when Failed or TimedOut
}

/// Every branch's outcome, in declaration order, and how the join went.
public sealed class BranchOutcomes<TState> : IReadOnlyList<BranchOutcome<TState>>
{
    public bool JoinMet { get; }
    /// The branches that met the join, in the order they did: the winner of a race, the first Count
    /// of a quorum, every branch for All. Empty when the join wasn't met.
    public IReadOnlyList<string> Deciding { get; }
    /// The succeeded branches' states, in declaration order.
    public IReadOnlyList<TState> Succeeded { get; }
    /// The outcome of the branch with this name, if the group has one.
    public BranchOutcome<TState>? this[string branch] { get; }
}

public sealed class StateGraph<TState>
{
    // Existing, unchanged:
    public StateGraph<TState> AddParallelGroup(string name, ParallelBranches<TState> branches,
        Func<TState, IReadOnlyList<TState>, TState> reducer, IReadOnlyDictionary<string, string>? metadata = null);

    // New: the plain reducer, with options. It gets the deciding branches' states when the join is
    // met, else every succeeded branch's, in declaration order.
    public StateGraph<TState> AddParallelGroup(string name, ParallelBranches<TState> branches,
        Func<TState, IReadOnlyList<TState>, TState> reducer, ParallelGroupOptions<TState> options,
        IReadOnlyDictionary<string, string>? metadata = null);

    // New: the outcomes reducer.
    public StateGraph<TState> AddParallelGroupWithOutcomes(string name, ParallelBranches<TState> branches,
        Func<TState, BranchOutcomes<TState>, TState> reducer, ParallelGroupOptions<TState>? options = null,
        IReadOnlyDictionary<string, string>? metadata = null);
}
```

### Rust

```rust
pub struct ParallelGroupOptions<S> { /* private */ }

impl<S> ParallelGroupOptions<S> {
    pub fn new() -> Self;
    /// `.compile()` refuses 0.
    pub fn with_max_concurrency(self, limit: usize) -> Self;
    /// `.compile()` refuses zero.
    pub fn with_branch_timeout(self, timeout: Duration) -> Self;
    pub fn with_cancel_siblings_on_failure(self, cancel: bool) -> Self;
    pub fn with_partial(self, partial: PartialResults) -> Self;
    pub fn keep_succeeded(self) -> Self;            // with_partial(PartialResults::KeepSucceeded)
    pub fn with_join(self, join: Join<S>) -> Self;
}

pub enum PartialResults { FailGroup, KeepSucceeded }  // FailGroup is the Default

pub struct Join<S> { /* private */ }

impl<S> Join<S> {
    pub fn all() -> Self;
    pub fn first_success() -> Self;
    pub fn first_matching(predicate: impl Fn(&S) -> bool + Send + Sync + 'static) -> Self;
    pub fn quorum(count: usize) -> Self;
    /// Exported as the join's condition (FirstMatching).
    pub fn described(self, description: impl Into<String>) -> Self;
    pub fn kind(&self) -> JoinKind;
}

pub enum JoinKind { All, FirstSuccess, FirstMatching, Quorum }

pub enum BranchStatus { Succeeded, Failed, TimedOut, Cancelled }

#[non_exhaustive]
pub struct BranchOutcome<S> {
    pub name: String,
    pub status: BranchStatus,
    pub state: Option<S>,
    pub error: Option<NodeError>,
}

pub struct BranchOutcomes<S> { /* private */ }

impl<S> BranchOutcomes<S> {
    pub fn outcomes(&self) -> &[BranchOutcome<S>];
    pub fn into_outcomes(self) -> Vec<BranchOutcome<S>>;
    pub fn join_met(&self) -> bool;
    pub fn deciding(&self) -> &[String];
    pub fn succeeded(&self) -> impl Iterator<Item = &S>;
    pub fn get(&self, branch: &str) -> Option<&BranchOutcome<S>>;
}

impl<S> StateGraph<S> {
    pub fn add_parallel_group(…);                              // unchanged
    pub fn add_parallel_group_with<R>(self, name, branches, reducer: R, options: ParallelGroupOptions<S>) -> Self
        where R: Fn(&S, Vec<S>) -> S + Send + Sync + 'static;
    pub fn add_parallel_group_with_outcomes<R>(self, name, branches, reducer: R, options: ParallelGroupOptions<S>) -> Self
        where R: Fn(&S, BranchOutcomes<S>) -> S + Send + Sync + 'static;
}
```

- The names differ from .NET's overloads only where Rust has no overloading: `add_parallel_group_with` is .NET's `AddParallelGroup(…, options)`.
- .NET's options type is `ParallelGroupOptions`, not `ParallelOptions`, which `System.Threading.Tasks` already has.
- The outcomes reducer is a separate method in .NET too: two `AddParallelGroup` overloads differing only in the reducer's second parameter would make `(entry, _) => entry` ambiguous.

---

## Semantics

### The join

| Join | Met when | Can no longer be met when | Deciding branches |
|---|---|---|---|
| `All` (default) | every branch succeeded | any branch failed | every branch |
| `FirstSuccess` | one branch succeeded | every branch failed | the first to succeed |
| `FirstMatching(p)` | a branch succeeded with `p(state)` true | every branch settled and none matched | the first to match |
| `Quorum(n)` | n branches succeeded | fewer than n can still succeed | the first n to succeed |

- **"First" is completion order**, the order the group saw branches settle. The reducer still gets everything in declaration order; `Deciding` keeps the completion order.
- **The predicate** runs on the branch's result state, on the executor, once per succeeded branch, when the group sees it settle. It must be cheap and must not throw; an exception from it is a bug and propagates like a reducer's.
- **When the join is met** the branches still running are cancelled (their token, as for a failing sibling today) and reported `cancelled` with reason `LostRace`; branches the concurrency limit hadn't started never start. The group then reduces at once: it doesn't wait for the losers, and a sync loser runs on with its result discarded (`design/14`).

### Failures and `Partial`

A branch failure (a `NodeException` / `NodeError`, or a branch timeout) only matters to the group once the join **can no longer be met**. Until then a race goes on: an `openai` error doesn't end `rca-race` while `anthropic` might still answer.

Once it can't be met:

- **`FailGroup`** (default): the group fails. With `CancelSiblingsOnFailure` (default) it fails at once, cancelling the branches still running (reason `SiblingFailed`): for `All` this is exactly today's behaviour. Without it, the group waits for them to settle, then fails, naming every failure.
- **`KeepSucceeded`**: the group waits for every branch to settle, then reduces with the join flagged unmet (`JoinMet` false, `Deciding` empty). If **no** branch succeeded, there is nothing to keep, and the group fails.

So `All` + `KeepSucceeded` is "every branch runs to the end, the reducer sees who failed": the specialists. `FirstMatching` + `KeepSucceeded` is "the first confident answer, or the best of all": FIRST_CONFIDENT.

A branch that throws anything but a `NodeException`, or panics, is a bug and propagates out of the run, as today, whatever the options.

### The group's failure

The message names each failed branch in declaration order (`branch 'kubernetes': timed out after 45000 ms; branch 'database': …`), as today, and says why the join wasn't met when that isn't "a branch failed": `no branch matched the join (3 succeeded)`, `quorum of 2 not reached: 1 of 3 branches succeeded; …`.

The failure is **transient if a retry could meet the join**, counting each transient failure (and timeout) as a branch that might succeed next time and each fatal one as a branch that won't:

- `All`: transient only if no branch failed fatally, which is today's rule.
- `FirstSuccess` / `FirstMatching`: transient if any branch failed transiently, or if every branch succeeded and none matched (a retry asks again, and model answers vary).
- `Quorum(n)`: transient if at least n branches didn't fail fatally.

A transient group failure carries the longest retry hint among its failed branches (`design/25`).

### Timeouts and the concurrency limit

- **`BranchTimeout`** runs from the moment the branch starts, on the run's clock (`RunOptions.TimeProvider` / tokio's, `design/25`), and is the branch's own: it cancels that branch's token only. The branch fails transiently with `branch '<name>' timed out after <ms> ms`, status `TimedOut`, and its `BranchCompleted` is `failed` with reason `TimedOut`. A sync branch can't see its token; the group stops waiting for it and it runs on, discarded.
- **`MaxConcurrency`** holds back branches beyond the limit; each starts, in declaration order, when a running branch settles. A held-back branch gets no events and no span until it starts.
- **The limit counts branches the group waits for, not code still running.** A branch the group gives up on (timed out, or failed while the group goes on) settles at once, and the next branch starts in its slot. The engine never waits for a handler it has given up on (design/17), so the old branch may still be running. A .NET branch is abandoned (told through its token, and no longer awaited), and so is a Rust sync branch, which can't see its token. A Rust async branch's future is dropped before its slot frees, so it has stopped. Code inside the branches that counts how many are in flight can therefore see one more than the limit for a moment. A branch that must not overlap the next should be async and stop when its token fires. A map's items wind down instead (design/27), so a map's `MaxConcurrency` is exact. Winding a group's branches down too was considered (2026-10-24, from demo 17) and declined: a branch that ignores its token would then hold the group past its timeout. When the group ends early (the join was met, the join can't be met under `FailGroup` with cancellation, the run was cancelled), branches that never started are reported to an outcomes reducer as `Cancelled` and get no events.
- **The group's own timeout** (`SetTimeout` on the group) still covers the whole group and fails it transiently, discarding every result. Use `BranchTimeout` to keep what finished.
- **A cancelled run** cancels every branch and ends the run `Cancelled`, as before; no reducer runs.

### Retries

**Per-branch retry is not built.** The group's `RecoveryPolicy` retries the group as a whole, which re-runs every branch, the ones that succeeded included, with the group's backoff (`design/25`). Under `KeepSucceeded` a group fails only when nothing succeeded, so a retry re-runs nothing that had succeeded. Branches must be safe to repeat, as `design/14` already says.

### Hops

A group is one hop at its level (`design/24`), whatever its options and however many branches it starts.

---

## Run events and telemetry

| | |
|---|---|
| `BranchCompleted` | Keeps `Succeeded` / `Failed` / `Cancelled`, and gains **`Reason`** (.NET `BranchEndReason?`, Rust `Option<BranchEndReason>`): **`TimedOut`** on a `failed` branch that ran over `BranchTimeout`; **`LostRace`** on a `cancelled` branch the join was met without; **`SiblingFailed`** on a `cancelled` branch a `FailGroup` group gave up on. No reason otherwise: a success, an ordinary failure, or a cancelled run or group timeout. Existing consumers see the same three results. |
| `JoinDecided` | **New**, for a group whose join isn't `All`, once per attempt, at the group's path: **`Policy`** (`first_success`, `first_matching`, `quorum`), **`Met`**, and **`Deciding`** (the branch names, in completion order; empty when unmet). It comes after the deciding branch's `BranchCompleted` and before the losers'. A console draws the winner from it and the losers from their `LostRace`. |
| Branch span | `strata.branch.reason` when a reason is set. |
| Group's attempt span | A `join_decided` span event (`tracing` `info` in Rust) with `strata.join.policy`, `strata.join.met` and `strata.join.deciding`. |
| Metrics | Unchanged: there is no branch metric, and the group's `strata.node.duration` covers its branches as before. |

An `All` group, with or without `KeepSucceeded`, emits no `JoinDecided`: every branch is in it, and `BranchCompleted` already says how each ended.

Event order for a race won by `anthropic`:

```text
NodeStarted rca-race#1
BranchStarted rca-race/openai, rca-race/anthropic, rca-race/local
BranchCompleted rca-race/openai Failed                (a 500: the race goes on)
BranchCompleted rca-race/anthropic Succeeded
JoinDecided rca-race first_matching met [anthropic]
BranchCompleted rca-race/local Cancelled LostRace
NodeCompleted rca-race#1 Committed
```

---

## Wire format

### The definition

A group with a non-default join or `Partial` exports a **`join`** object:

```json
{
  "name": "race",
  "metadata": null,
  "kind": "parallel_group",
  "branches": [ { "name": "openai", "metadata": null }, … ],
  "join": { "policy": "first_matching", "count": null, "condition": "validated, confidence >= 0.9", "partial": "keep_succeeded" }
}
```

- `policy`: `all`, `first_success`, `first_matching` or `quorum`. `count`: the quorum, `null` for every other policy. `condition`: `FirstMatching`'s description, or `null`. `partial`: `fail_group` or `keep_succeeded`. Every key is always written, like an edge's `condition`.
- A group with the defaults (`All`, `FailGroup`) writes no `join`, so every existing definition is byte for byte as before.
- **A level with a `join` is `schema_version` `1.2.0`**, the lowest version that can express it, as `1.1.0` was for a `kind` (`design/22`). Readers accept `1.0.0`, `1.1.0` and `1.2.0`; `check_shape` refuses a `join` below `1.2.0`, or on anything but a `parallel_group`, or with a `count` on a policy other than `quorum` or a `condition` on one other than `first_matching`, with exit 2 in the CLI.
- `MaxConcurrency`, `BranchTimeout` and `CancelSiblingsOnFailure` are not exported: they tune how a group runs, not what it means, like a node's timeout and recovery policy.

The shared validator gains **`STRATA_ERR_INVALID_QUORUM`**: a `quorum` join whose `count` is missing, below 1, or above the group's branch count, named by the group's qualified path. It is reported with the group checks (`STRATA_ERR_EMPTY_GROUP`, `STRATA_ERR_DUPLICATE_BRANCH`), so `.compile()` refuses such a graph and `strata graph validate` fails a file with one.

New fixtures: `schema/fixtures/graph-export/joins.json` (one group per policy and a `KeepSucceeded` `All` group, exported by both engines and validated by the CLI) and `schema/fixtures/graph-validation/invalid-quorum.json`. `strata graph view` adds the join to a group's annotation.

### Checkpoints

**Unchanged.** A group is one node to the graph around it: its branches, their outcomes and the join's decision exist only inside one attempt, so nothing about them is persisted, and a resume re-runs the whole group, as before.

---

## Migration

**.NET** (source-compatible): `AddParallelGroup` without options is unchanged. `ParallelGroupOptions<TState>`, `PartialResults`, `JoinPolicy<TState>`, `JoinKind`, `BranchStatus`, `BranchOutcome<TState>`, `BranchOutcomes<TState>`, `AddParallelGroup(…, options)`, `AddParallelGroupWithOutcomes`, `BranchCompleted.Reason`, `BranchEndReason` and the `JoinDecided` event are new. `GraphNodeDetail` gains `Join`.

**Rust:** `add_parallel_group` is unchanged. `RunEventKind::BranchCompleted` gains a `reason` field, so a pattern that names its fields needs `..`. `GraphNodeDef` gains `join: Option<GraphJoinDef>`, so a struct literal needs it (or `..Default::default()`). The rest is new.

**StrataOps:** `investigate` uses `AddParallelGroupWithOutcomes` with `BranchTimeout`, `MaxConcurrency` and `KeepSucceeded`, and its branches stop catching their own failures. `rca-race` uses `FirstMatching` + `KeepSucceeded` for `FIRST_CONFIDENT`, and `All` / `Quorum` for `MAJORITY` / `CONSENSUS`. The console reads `JoinDecided` and the `LostRace` reason.

---

## Tests

Mirrored per engine, in new suites `group_controls.rs` / `GroupControlsTests.cs` (SG-07) and `group_joins.rs` / `GroupJoinTests.cs` (SG-08), on a controlled clock where time matters:

- **SG-07:**
  - **The concurrency limit is respected:** the peak number of branches in flight equals the limit, and branches start in declaration order.
  - **A branch timeout times out only that branch:** it is `TimedOut`, its siblings finish, and its `BranchCompleted` is `failed` with `TimedOut`; a queued branch's timeout starts when it starts.
  - **Partial results reach the reducer:** under `KeepSucceeded` the outcomes reducer gets every branch in declaration order, with statuses, states and errors; the plain reducer gets the succeeded states.
  - **All failed fails the group**, transiently when a failure was transient, with the longest hint.
  - **Opting out of sibling cancellation:** under `FailGroup` without it, every branch finishes and the message names every failure; with it (the default), siblings are cancelled with `SiblingFailed`.
  - **No options is today's behaviour**, event for event.
- **SG-08:**
  - **Each join policy:** `FirstSuccess`, `FirstMatching` and `Quorum(2)` reduce as soon as they are met, with `Deciding` in completion order; `All` with options behaves as `All` without.
  - **Losers are cancelled and reported:** their tokens fire, they are `cancelled` with `LostRace`, held-back branches never start, and `JoinDecided` sits between the winner's `BranchCompleted` and the losers'.
  - **A race tolerates failures** until it can't be won, then fails, transiently if any failure was.
  - **No match falls back:** `FirstMatching` + `KeepSucceeded` reduces over every outcome with `JoinMet` false; under `FailGroup` the group fails with "no branch matched".
  - **A race inside a subgraph:** paths are `<subgraph>/<group>/<branch>`, and the child's `JoinDecided` reaches the run's events.
  - **Export and validation:** `joins.json` from both engines; a default group exports no `join` and stays `1.1.0`; `STRATA_ERR_INVALID_QUORUM` from `.compile()`, both validators and the CLI; `check_shape` refuses a misplaced `join`.
- **Mutation checks:** not cancelling losers, failing a race on its first failure, and passing outcomes in completion order should each fail tests in both engines.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

### The questions as asked

Recommendations are in bold.

1. **One options type?** **Yes: `ParallelGroupOptions<TState>`, generic because a `FirstMatching` predicate reads the state.** The map (`design/27`) reuses it.
2. **What does the group do when every branch failed?** **It fails, transiently if a retry could meet the join** (for a race, any transient failure; for `All`, no fatal one), whatever `Partial` says.
3. **Is a branch timeout transient?** **Yes**, like a node timeout.
4. **Per-branch retry?** **Not built.** The group's policy retries the group, which re-runs every branch.
5. **Joins:** **`All`, `FirstSuccess`, `FirstMatching(predicate)`, `Quorum(n)`**; losers cancelled with `LostRace`; the reducer gets every outcome plus `Deciding`.
6. **No match?** The handoff recommended falling back to all results with a flag. **The build makes that `Partial`'s job:** under `KeepSucceeded` an unmet join reduces over all results with `JoinMet` false, which is FIRST_CONFIDENT's fallback; under the default `FailGroup` it fails the group. One rule then covers every join (an unmet `Quorum` or an all-failed `All` behaves the same way), and a race whose author didn't ask for partial results doesn't silently reduce over answers that didn't qualify.
7. **A reason on `BranchCompleted`, or new results?** **A reason**, so existing consumers are unaffected.
8. **The join's decision as an event?** **Yes, `JoinDecided`**, because with `FirstMatching` a succeeded branch isn't necessarily the winner, and a `Quorum`'s deciding set isn't visible from the branch events alone.
9. **Export?** **The join and `Partial`, yes (`1.2.0`); limits and timeouts, no.**
10. **Hops?** **One per group.**

---

## Implementation Status

**Since then (2026-10-24):** "Timeouts and the concurrency limit" now says that `MaxConcurrency` counts the branches the group waits for, so an abandoned branch's code may overlap the next. The .NET demo 17 counted 3 in flight under a limit of 2. Documented rather than changed; no behaviour changed.

Built 2026-10-05 in both engines, the shared validator, the schema and the CLI, in one change, as specified above. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**Where it diverged, or filled a gap, and why:**

- **Validation of the options is each engine's idiom.** .NET's `ParallelGroupOptions` setters throw `ArgumentOutOfRangeException` for a `MaxConcurrency` below 1 or a non-positive `BranchTimeout`; Rust's builder can't fail, so `.compile()` refuses them (`CompilationFailed`). A quorum's count is checked by the shared validator in both, since it depends on the branch count and is exported.
- **`JoinKind` and `PartialResults` are the definition's types too.** Both engines serialize the runtime enums (`first_success`, `keep_succeeded`, …) instead of mirroring them in the interop types, so the two can't drift.
- **`described` is ignored except on `FirstMatching`** in Rust, and .NET's `Description` is only settable through `FirstMatching`, so a `condition` is never exported for a policy the schema forbids it on.
- **A branch's token is its own.** Each branch gets a child of the group's siblings token, cancelled by its own timeout, so `BranchTimeout` stops that branch only. In Rust every branch now runs as a task in the `JoinSet` that races its work against its token and its timeout; a sync branch's work is still on the blocking pool, detached when the group gives up on it.
- **Losers are reported at the moment the join is decided**, in declaration order; a branch that had already settled when the group looked (a sync branch that finished in the same instant) is reported as what it was, not as a loser.
- **An unknown `schema_version` in the CLI tests moved from `1.2.0` to `1.9.0`**, since `1.2.0` is now known.

**How it was built:**

- **.NET:**
  - `Graph/ParallelGroupOptions.cs` is new: `ParallelGroupOptions<TState>`, `PartialResults`, `JoinKind`, `JoinPolicy<TState>`, `BranchStatus`, `BranchOutcome<TState>`, `BranchOutcomes<TState>`.
  - `Graph/ParallelBranches.cs`: `ParallelGroupRunner.RunAsync` returns the outcomes; it starts branches up to the limit, settles each through a wrapper task that never throws (succeeded, failed, timed out, cancelled, or a bug), counts them towards the join, and decides when it is met or unreachable. `Failure` builds the group's error. `GroupShape` carries the exported join.
  - `StateGraph.cs`: `AddParallelGroup(…, options)` and `AddParallelGroupWithOutcomes` share `AddGroup`; the original overload passes default options. `GraphTopology.cs`: `GraphJoin` and `GraphNodeDetail.Join`.
  - `Validation`: `ValidationErrorKind.InvalidQuorum` and the check in `AddGroupErrors`.
  - `Observability`: `BranchEndReason`, `BranchCompleted.Reason`, `JoinDecided`, `BranchScope.Completed`'s reason and `RunTelemetry.JoinDecided`.
  - `Datalium.Strata.Storage/Schema`: `GraphJoinDefinition`, `GraphDefinition.JoinVersion`, the version rule in `ToDefinition`, and `CheckJoinFits` in `ToTopology`.
- **Rust:**
  - `parallel.rs`: the same types (`Join<S>` for .NET's `JoinPolicy`), and `run_branches` rebuilt around a `GroupRun` that owns the `JoinSet`, the scopes and the join's count.
  - `graph.rs`: `add_parallel_group_with` and `add_parallel_group_with_outcomes` share `add_group`, which stores one outcomes reducer; `add_parallel_group` passes default options. `.compile()` refuses a zero limit or timeout; `node_definition` exports the join.
  - `schema.rs`: `GraphJoinDef`, `GraphNodeDef::join`, `JOIN_GRAPH_SCHEMA_VERSION`, and the join rules in `check_shape`. `validate.rs`: `InvalidQuorum`. `observability.rs`: as .NET.
- **CLI:** `graph view` annotates a group's join; `graph validate` reports the new error through the existing paths.
- **Schema:** `graph-definition.schema.json` gains `join` and `1.2.0`; new fixtures `graph-export/joins.json` and `graph-validation/invalid-quorum.json`, both checked against the JSON Schema.

**Tests:**

- **New suites:** `GroupControlsTests.cs` (10) / `group_controls.rs` (10), and `GroupJoinTests.cs` (19) / `group_joins.rs` (16). The join suites differ only in shape: .NET's misplaced-join check is a theory with three cases plus a test for a join on a plain node, where Rust asserts all four in one test. The CLI gained three tests (`joins.json` and `invalid-quorum.json` validated, a join below `1.2.0` refused, the join annotated by `view`).
- **Timing:** Rust's suites run on tokio's paused clock, so they are deterministic. .NET's timeout tests run on a `FakeTimeProvider`; its race tests order branches with short real delays (0, 30 ms, or never), and passed five repeated runs.
- **Totals when this landed:** .NET 514 passed, with 38 Postgres/Redis tests skipped because those services weren't running locally (this change touches no store), plus the example's 1. `rust/` 508, `cli/` 61, `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` (all three projects) and `dotnet format --verify-no-changes` are clean, and every definition fixture validates against `graph-definition.schema.json`.
- **Mutation checks, in both engines:**
  - Not cancelling the losers when the join is met fails 4 tests in each.
  - Failing a race on its first failure (treating every join as `All`) fails 2 in each.
  - Handing the plain reducer the deciding states in completion order fails 4 in .NET and 2 in Rust (.NET's older group suite checks the order in more places).

## Not built (deliberately out of scope)

- **Per-branch retry policies** and per-branch backoff. The group retries as a whole.
- **Keeping partial results across a group retry**, a crash or a resume. A retry or a resume re-runs every branch (SG-10 for crash-safe progress).
- **Branches as graph nodes**, per-branch interrupt boundaries, and nested groups (a branch can't hold a group; a map's child graph can, `design/27`).
- **Exporting limits and timeouts**, or the predicate itself (only its description).
- **Join policies beyond these four** (a weighted vote, a best-of with a deadline): a StrataOps reducer over `All` or `Quorum` outcomes covers MAJORITY and CONSENSUS, and model routing strategies are StrataOps' (SG-24).
- **A minimum wait before a race is decided** (collect answers for N seconds, then pick). `BranchTimeout` with `KeepSucceeded` comes close.
