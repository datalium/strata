# Datalium Strata: Distributed Execution (a Decision, Not a Build)

**SG-26** (StrataOps' gap log) asks for "worker coordination, placement and cluster membership (Arch §6.2, §18)": a Strata that spreads runs across processes itself. The gaps doc already called a cluster runtime "a large, separate product decision". **Nick closed SG-26 by decision on 2026-10-21: Strata does not distribute work.** An application distributes runs with whatever it already uses (Kafka partitions, a queue, a job system), and Strata makes every run safe to place anywhere. This document records what that means, what Strata provides for it, and what would reopen the question.

It is a decision record: nothing is built, and no API, wire format or package changes.

---

## The decision

| Question | Decision |
|---|---|
| Does Strata place runs on workers? | **No.** Placement, affinity, partitioning and membership are the application's, through its own broker or queue. |
| Does Strata have a cluster runtime or a run queue? | **No.** A store-backed run queue (durable enqueue, workers claiming runs under leases, cross-process priorities and tenants) was the alternative. It is recorded below as what would reopen SG-26, not built. |
| What does Strata guarantee instead? | **That a thread is safe wherever it runs:** one writer at a time, fenced; crashed runs recovered by any replica; drivers that every replica can run. These are SG-10, SG-12, SG-30 and SG-28, all built. |
| Scheduling across processes | **Per process.** The run scheduler (design/40, SG-25) limits concurrency, priority and tenants within one process. Across processes, the application's partitioning bounds the load each process takes. |

---

## Why

- **The application already has a distributor.** StrataOps' Worker consumes `ops.investigations` from Redpanda (the Kafka API) with the incident id as the partition key, and scales by partitions (plan §3, "Horizontally, by Kafka partitions"). Every application that runs agents at scale has a queue or broker of its own. A second one inside Strata would compete with it, not replace it.
- **The hard part was correctness, and that is built.** Two processes on one thread must not both write it, a crashed process's thread must continue, and timers and recoveries must fire once. Leases with fencing (design/32), durable checkpoints with recovery (design/33), conditional saves (design/28) and the hosting drivers (design/35) do this for any placement, without Strata knowing where runs are.
- **A cluster runtime is a product, not a gap.** Membership, placement, rebalancing, affinity (Arch §6.2's GPU, tool locality, data residency and security zone) and an operations surface for all of it is the scope of a system like Temporal's or Ray's. It would need its own design, its own operations story and its own release line. Doing it as "a gap" would underbuild it.
- **Strata Cloud is where hosted execution would belong**, if it is ever wanted. Cloud already holds checkpoints, leases, the listing and decisions (design/30, design/43), so a hosted worker pool is a Cloud product decision rather than a library feature.

---

## How an application distributes runs on Strata

What follows is the pattern StrataOps uses, written down as the supported one.

1. **Partition by thread.** Route every message about a thread (start, approval, signal, wake) to the same partition, or the same consumer, with the thread id (or a key that determines it) as the partition key. Usually one process then owns a thread, which keeps the lease uncontended.
2. **Run every thread under a lease** (`RunOptions.Lease` / `RunConfig::with_lease`, or hosting's `Strata:Runs:Lease`). Partition handoffs during a rebalance can briefly give two processes the same thread. The lease refuses the second one (`Failed("thread 't' is held by …")`, nothing written), and fencing stops a process that lost its lease from writing again (`Cancelled(LeaseLost)`). Every worker on a thread must use leases, or none.
3. **Checkpoint every node where losing work matters** (`CheckpointPolicy.EveryNode`), and run the recovery driver (`AddRecoveryDriver` / `Host::recover`) on every replica. A thread whose process died is `running` and stale. Any replica recovers it, at-least-once, with `NodeContext.IdempotencyKey` for effects that must not repeat.
4. **Run the wake driver on every replica** (`AddWakeDriver` / `Host::wake`). Timers and timeouts are claimed by a conditional save, so two replicas never wake one thread twice. To keep a wake on the owning partition instead, an application can wake through its own resume (`LevelSignals.Due`, design/42) when its own scheduler fires.
5. **Bound each process with the run scheduler** (`AddScheduler`, design/40): priorities, reserved slots and tenant limits within the process. Cluster-wide limits are the partition count times the per-process limit, which is the application's sizing.
6. **Answer decisions where the thread lives.** The decision worker takes an `Owns` predicate (`ThreadIdPrefix` / `Owns`), so each replica answers the decisions for its own partitions. With leases, a worker that doesn't own the thread leaves the decision for the next round.

None of this needs a Strata change. The CLI (`thread list --status running`, `thread inspect`'s `Lease:` line) shows where things stand across the cluster from the store.

---

## What would reopen SG-26

A **store-backed run queue** is the design this decision deferred. A sketch, so the next discussion starts from it:

- **The queue:** a durable `EnqueueAsync(threadId, RunRequest)` into the checkpoint store's database, with a new table or Redis stream, so no broker is needed.
- **Workers:** each claims queued runs under SG-12's leases, in the scheduler's order (priority, then tenant fairness) across processes.
- **What already exists:** the drivers' claim and lease machinery covers most of it.
- **What's new:** a wire format for queued runs in every store and both engines, a claim protocol with visibility timeouts, dead-lettering, and a `strata queue` surface in the CLI.

It becomes worth building when one of these holds:

- **a second application needs distribution without a broker**, so a queue in the store it already has would save it one;
- **Strata Cloud decides to host workers**, so the queue lives in Cloud and both engines' workers claim from it;
- **cross-process admission becomes a requirement** (a global tenant limit that per-process limits can't approximate). That is a distributed scheduler, which design/40 left out for the same reason.

Placement by resource class, affinity and anti-affinity, and preemption (Arch §6.2) remain outside Strata even then: they belong to the platform the workers run on, such as Kubernetes or a GPU scheduler.

---

## Implementation Status

**Closed by decision on 2026-10-21. Nothing built.** StrataOps' gap log should mark SG-26 closed by decision at the commit that adds this record.
