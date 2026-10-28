using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Storage.Postgres;
using Microsoft.Extensions.Time.Testing;
using Npgsql;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Group commit (<c>design/41-Checkpoint-Efficiency.md</c>, SG-15): the queue on its own, then the SQLite and
/// Postgres stores with it on. Mirrors <c>strata-memory</c>'s <c>tests/group_commit.rs</c>. The Postgres tests
/// need <c>STRATA_TEST_POSTGRES_URL</c>.
/// </summary>
public class GroupCommitTests
{
    /// <summary>How long a test waits for something that should happen at once before it fails.</summary>
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    private static readonly GroupCommitOptions On = new();

    /// <summary>One flush at a time: what a test that holds a flush open, so the saves after it queue, needs.</summary>
    private static readonly GroupCommitOptions OneAtATime = new() { MaxConcurrentFlushes = 1 };

    private static Checkpoint Saved(string threadId, int n) =>
        StoreContracts.Checkpoint(threadId, $"2026-10-15T00:{n / 60:00}:{n % 60:00}Z");

    /// <summary>Records <c>strata.store.flush.size</c> for one store while it lives.</summary>
    private sealed class FlushSizes : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly ConcurrentQueue<int> _sizes = new();

        public FlushSizes(string store)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == StorageTelemetry.Name && instrument.Name == "strata.store.flush.size")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<int>((_, size, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "strata.store" && (string?)tag.Value == store)
                    {
                        _sizes.Enqueue(size);
                    }
                }
            });
            _listener.Start();
        }

        public IReadOnlyList<int> Sizes => [.. _sizes];

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// A fault hook that holds the first flush that reaches <paramref name="point"/> after <see cref="Arm"/>
    /// until <see cref="Release"/>, so the saves made meanwhile all wait for the next flush.
    /// </summary>
    private sealed class FlushHold(string point)
    {
        private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        public Func<string, ValueTask>? Then { get; init; }

        public void Arm() => Volatile.Write(ref _armed, 1);

        public Task Held => _held.Task.WaitAsync(Patience);

        public void Release() => _released.TrySetResult();

        public async ValueTask Fault(string reached)
        {
            if (reached == point && Interlocked.Exchange(ref _armed, 0) == 1)
            {
                _held.TrySetResult();
                await _released.Task.WaitAsync(Patience).ConfigureAwait(false);
            }

            if (Then is { } then)
            {
                await then(reached).ConfigureAwait(false);
            }
        }
    }

    // --- The queue on its own ---

    /// <summary>A flush that records its batches and saves every row, after <paramref name="hold"/> if given.</summary>
    private static (GroupCommitQueue Queue, ConcurrentQueue<string[]> Batches) Queue(
        GroupCommitOptions options, Func<IReadOnlyList<QueuedSave>, Task>? hold = null, Func<QueuedSave, bool>? bad = null)
    {
        var batches = new ConcurrentQueue<string[]>();
        var queue = new GroupCommitQueue(
            options,
            "test",
            async saves =>
            {
                batches.Enqueue([.. saves.Select(save => $"{save.Checkpoint.ThreadId}@{save.Checkpoint.CreatedAt[^3..^1]}")]);
                if (hold is not null)
                {
                    await hold(saves);
                }

                if (bad is not null && saves.Any(bad))
                {
                    throw new InvalidOperationException("a row the database refused");
                }

                foreach (var save in saves)
                {
                    save.Settle(SaveVerdict.Saved);
                }
            },
            isTransient: ex => ex is IOException);
        return (queue, batches);
    }

    [Fact]
    public async Task A_Save_Alone_Is_Flushed_At_Once_And_Alone()
    {
        var (queue, batches) = Queue(OneAtATime);

        Assert.True(await queue.SaveAsync(Saved("a", 1), null, null, default).AsTask().WaitAsync(Patience));

        Assert.Equal([["a@01"]], batches);
    }

    [Fact]
    public async Task Saves_That_Arrive_During_A_Flush_Go_Together_Up_To_The_Batch_Size()
    {
        var first = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var (queue, batches) = Queue(
            new GroupCommitOptions { MaxBatch = 3, MaxConcurrentFlushes = 1 },
            async saves =>
            {
                if (saves[0].Checkpoint.ThreadId == "first")
                {
                    first.TrySetResult();
                    await release.Task;
                }
            });

        var blocker = queue.SaveAsync(Saved("first", 0), null, null, default).AsTask();
        await first.Task.WaitAsync(Patience);
        var waiting = Enumerable.Range(1, 5).Select(i => queue.SaveAsync(Saved($"t{i}", i), null, null, default).AsTask()).ToList();
        Assert.Single(batches);
        release.SetResult();
        await Task.WhenAll([blocker, .. waiting]).WaitAsync(Patience);

        Assert.Equal([["first@00"], ["t1@01", "t2@02", "t3@03"], ["t4@04", "t5@05"]], batches);
    }

    [Fact]
    public async Task A_Threads_Saves_Keep_Their_Order_And_Wait_For_Its_Running_Flush()
    {
        var first = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var (queue, batches) = Queue(
            new GroupCommitOptions { MaxConcurrentFlushes = 2 },
            async saves =>
            {
                if (saves.Any(save => save.Checkpoint.CreatedAt.EndsWith(":00Z", StringComparison.Ordinal)))
                {
                    first.TrySetResult();
                    await release.Task;
                }
            });

        var blocker = queue.SaveAsync(Saved("t", 0), null, null, default).AsTask();
        await first.Task.WaitAsync(Patience);

        // A second flush may run, but not with t's next saves: they wait for t's running flush to end.
        var later = queue.SaveAsync(Saved("t", 1), null, null, default).AsTask();
        var other = queue.SaveAsync(Saved("u", 1), null, null, default).AsTask();
        await other.WaitAsync(Patience);
        var last = queue.SaveAsync(Saved("t", 2), null, null, default).AsTask();
        Assert.False(later.IsCompleted);
        release.SetResult();
        await Task.WhenAll(blocker, later, last).WaitAsync(Patience);

        Assert.Equal([["t@00"], ["u@01"], ["t@01", "t@02"]], batches);
    }

    [Fact]
    public async Task A_Flush_That_Fails_For_Good_Is_Tried_Again_Save_By_Save_So_Only_The_Bad_Save_Fails()
    {
        var first = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var (queue, batches) = Queue(
            OneAtATime,
            async saves =>
            {
                if (saves[0].Checkpoint.ThreadId == "first")
                {
                    first.TrySetResult();
                    await release.Task;
                }
            },
            bad: save => save.Checkpoint.ThreadId == "bad");

        var blocker = queue.SaveAsync(Saved("first", 0), null, null, default).AsTask();
        await first.Task.WaitAsync(Patience);
        var good = queue.SaveAsync(Saved("good", 1), null, null, default).AsTask();
        var bad = queue.SaveAsync(Saved("bad", 2), null, null, default).AsTask();
        var alsoGood = queue.SaveAsync(Saved("also", 3), null, null, default).AsTask();
        release.SetResult();

        Assert.True(await good.WaitAsync(Patience));
        Assert.True(await alsoGood.WaitAsync(Patience));
        Assert.Equal("a row the database refused", (await Assert.ThrowsAsync<InvalidOperationException>(() => bad)).Message);
        await blocker;
        Assert.Equal([["first@00"], ["good@01", "bad@02", "also@03"], ["good@01"], ["bad@02"], ["also@03"]], batches);
    }

    [Fact]
    public async Task A_Transient_Failure_Fails_Every_Save_Of_The_Flush()
    {
        var queue = new GroupCommitQueue(On, "test", _ => throw new IOException("gone"), isTransient: ex => ex is IOException);

        await Assert.ThrowsAsync<IOException>(() => queue.SaveAsync(Saved("a", 1), null, null, default).AsTask());
    }

    [Fact]
    public async Task A_Verdict_Reaches_Its_Own_Caller_Only()
    {
        var lease = new ThreadLease("fenced", "w", 1, DateTimeOffset.UnixEpoch);
        var queue = new GroupCommitQueue(
            OneAtATime,
            "test",
            saves =>
            {
                foreach (var save in saves)
                {
                    save.Settle(save.Checkpoint.ThreadId switch
                    {
                        "lost" => SaveVerdict.Lost,
                        "fenced" => SaveVerdict.LeaseLost,
                        _ => SaveVerdict.Saved,
                    });
                }

                return ValueTask.CompletedTask;
            },
            isTransient: _ => false);

        Assert.False(await queue.SaveAsync(Saved("lost", 1), 0, null, default));
        Assert.Same(lease, (await Assert.ThrowsAsync<LeaseLostException>(() => queue.SaveAsync(Saved("fenced", 1), null, lease, default).AsTask())).Lease);
        Assert.True(await queue.SaveAsync(Saved("fine", 1), null, null, default));
    }

    [Fact]
    public async Task A_Queued_Save_Is_Withdrawn_By_Its_Token_And_A_Flushing_One_Is_Not()
    {
        var first = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var (queue, batches) = Queue(
            OneAtATime,
            async saves =>
            {
                if (saves[0].Checkpoint.ThreadId == "first")
                {
                    first.TrySetResult();
                    await release.Task;
                }
            });
        using var flushing = new CancellationTokenSource();
        using var queued = new CancellationTokenSource();

        var blocker = queue.SaveAsync(Saved("first", 0), null, null, flushing.Token).AsTask();
        await first.Task.WaitAsync(Patience);
        var withdrawn = queue.SaveAsync(Saved("gone", 1), null, null, queued.Token).AsTask();
        var kept = queue.SaveAsync(Saved("kept", 2), null, null, default).AsTask();
        queued.Cancel();
        flushing.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => withdrawn.WaitAsync(Patience));
        release.SetResult();

        Assert.True(await blocker.WaitAsync(Patience));
        Assert.True(await kept.WaitAsync(Patience));
        Assert.Equal([["first@00"], ["kept@02"]], batches);
    }

    [Fact]
    public async Task Draining_Flushes_What_Is_Queued_And_Refuses_Later_Saves()
    {
        var first = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var (queue, batches) = Queue(
            OneAtATime,
            async saves =>
            {
                if (saves[0].Checkpoint.ThreadId == "first")
                {
                    first.TrySetResult();
                    await release.Task;
                }
            });

        var blocker = queue.SaveAsync(Saved("first", 0), null, null, default).AsTask();
        await first.Task.WaitAsync(Patience);
        var queued = queue.SaveAsync(Saved("queued", 1), null, null, default).AsTask();
        var drained = queue.DrainAsync().AsTask();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => queue.SaveAsync(Saved("late", 2), null, null, default).AsTask());
        Assert.False(drained.IsCompleted);
        release.SetResult();

        await drained.WaitAsync(Patience);
        // A save's answer reaches its caller on the thread pool, a moment after the drain: await, don't look.
        await Task.WhenAll(queued, blocker).WaitAsync(Patience);
        Assert.Equal([["first@00"], ["queued@01"]], batches);
    }

    [Fact]
    public void The_Options_Are_Checked()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new GroupCommitOptions { MaxBatch = 0 }.Checked());
        Assert.Throws<ArgumentOutOfRangeException>(() => new GroupCommitOptions { MaxConcurrentFlushes = 0 }.Checked());
        Assert.Equal((256, 4), (On.MaxBatch, On.MaxConcurrentFlushes));
    }

    // --- The stores ---

    /// <summary>A store with group commit on, the point its fault hook can hold a flush at, and its cleanup.</summary>
    private sealed record Backend(string Name, ICheckpointStore Store, IDisposable Cleanup);

    private static Backend Sqlite(Func<string, ValueTask>? fault = null, GroupCommitOptions? options = null)
    {
        var store = EfCoreCheckpointStore.CreateInMemory(
            new FakeTimeProvider(DateTimeOffset.UtcNow), new SqliteStoreOptions { GroupCommit = options ?? On, Fault = fault });
        return new Backend("sqlite", store, store);
    }

    private static Backend Postgres(string label, Func<string, ValueTask>? fault = null, GroupCommitOptions? options = null)
    {
        var sandbox = new PostgresSandbox(label);
        var store = PostgresSandbox.Open(
            sandbox.CreateDataSource(), new PostgresStoreOptions { GroupCommit = options ?? On, Fault = fault });
        return new Backend("postgres", store, new Both(store, sandbox));
    }

    private sealed class Both(IDisposable first, IDisposable second) : IDisposable
    {
        public void Dispose()
        {
            first.Dispose();
            second.Dispose();
        }
    }

    /// <summary>Where each store's fault hook can hold a flush before it commits.</summary>
    private static string HoldPoint(string store) => store == "sqlite" ? "flush:start" : "save:before-commit";

    private static async Task<List<string>> Log(ICheckpointStore store, string threadId) =>
        [.. (await ((ICheckpointQuery)store).GetLogAsync(threadId, limit: ThreadQuery.MaxLimit)).Entries
            .Select(entry => entry.Checkpoint.CreatedAt[^3..^1])];

    [Fact]
    public async Task Sqlite_Answers_Each_Row_Of_A_Flush_On_Its_Own() => await EachRowAnsweredAsync("sqlite", hold => Sqlite(hold.Fault, OneAtATime));

    [PostgresFact]
    public async Task Postgres_Answers_Each_Row_Of_A_Flush_On_Its_Own() =>
        await EachRowAnsweredAsync("postgres", hold => Postgres("rows", hold.Fault, OneAtATime));

    /// <summary>
    /// One flush holding a conditional save that lost its race, a fenced save whose lease was taken over, one
    /// whose lease is live, plain saves, two saves of one thread and a conditional save that wins after an
    /// earlier row of its thread was refused: each gets its own answer, and every log is as if they had been
    /// made one by one.
    /// </summary>
    private static async Task EachRowAnsweredAsync(string name, Func<FlushHold, Backend> open)
    {
        var hold = new FlushHold(HoldPoint(name));
        var backend = open(hold);
        using var cleanup = backend.Cleanup;
        var store = backend.Store;
        using var sizes = new FlushSizes(backend.Name);

        await store.SaveAsync(Saved("cas", 0));
        await store.SaveAsync(Saved("cas", 1));
        var stale = (await store.TryAcquireLeaseAsync("fenced", "w1", TimeSpan.FromMinutes(5))).Lease!;
        Assert.True(await store.ReleaseLeaseAsync(stale));
        var live = (await store.TryAcquireLeaseAsync("fenced", "w2", TimeSpan.FromMinutes(5))).Lease!;
        Assert.Equal(stale.Token + 1, live.Token);

        hold.Arm();
        var blocker = store.SaveAsync(Saved("blocker", 0)).AsTask();
        await hold.Held;
        var lost = store.SaveIfLatestAsync(Saved("cas", 2), 0).AsTask();
        var fencedOut = store.SaveUnderLeaseAsync(Saved("fenced", 0), stale).AsTask();
        var plain = store.SaveAsync(Saved("plain", 0)).AsTask();
        var pairFirst = store.SaveAsync(Saved("pair", 0)).AsTask();
        var pairSecond = store.SaveAsync(Saved("pair", 1)).AsTask();
        var wins = store.SaveIfLatestAsync(Saved("cas", 3), 1).AsTask();
        var fencedIn = store.SaveUnderLeaseAsync(Saved("fenced", 1), live).AsTask();
        var blockerAgain = store.SaveAsync(Saved("blocker", 1)).AsTask();
        hold.Release();

        Assert.False(await lost.WaitAsync(Patience));
        Assert.Equal(stale.Token, (await Assert.ThrowsAsync<LeaseLostException>(() => fencedOut.WaitAsync(Patience))).Lease.Token);
        await Task.WhenAll(blocker, plain, pairFirst, pairSecond, blockerAgain).WaitAsync(Patience);
        Assert.True(await wins.WaitAsync(Patience));
        Assert.True(await fencedIn.WaitAsync(Patience));

        Assert.Equal(["00", "01", "03"], await Log(store, "cas"));
        Assert.Equal(["01"], await Log(store, "fenced"));
        Assert.Equal(["00", "01"], await Log(store, "pair"));
        Assert.Equal(["00", "01"], await Log(store, "blocker"));
        Assert.Equal(["00"], await Log(store, "plain"));

        // The blocker's later save waited for its first flush to end, so it went with the rest.
        Assert.Contains(8, sizes.Sizes);
    }

    [Fact]
    public async Task Sqlite_Keeps_Every_Log_Whole_Under_Concurrent_Writers() =>
        await ConcurrentWritersAsync("sqlite", hold => Sqlite(hold.Fault));

    [PostgresFact]
    public async Task Postgres_Keeps_Every_Log_Whole_Under_Concurrent_Writers() =>
        await ConcurrentWritersAsync("postgres", hold => Postgres("writers", hold.Fault));

    /// <summary>
    /// Saves of many threads from many writers at once: every log whole and in order, and a flush that took more
    /// than one save. Each writer's first save is made while a flush is held open, so it queues behind it: otherwise,
    /// on a busy machine, every save can find no flush running and flush alone (an in-memory SQLite flush runs on the
    /// caller's thread and is over before the next writer gets one), and the test would say nothing about batching.
    /// </summary>
    private static async Task ConcurrentWritersAsync(string name, Func<FlushHold, Backend> open)
    {
        var hold = new FlushHold(HoldPoint(name));
        var backend = open(hold);
        using var cleanup = backend.Cleanup;
        using var sizes = new FlushSizes(backend.Name);
        const int Writers = 24;
        const int Saves = 20;

        hold.Arm();
        var blocker = backend.Store.SaveAsync(Saved("blocker", 0)).AsTask();
        await hold.Held;
        var firsts = Enumerable.Range(0, Writers).Select(writer => backend.Store.SaveAsync(Saved($"w{writer}", 0)).AsTask()).ToList();
        hold.Release();
        await blocker.WaitAsync(Patience);

        await Task.WhenAll(Enumerable.Range(0, Writers).Select(writer => Task.Run(async () =>
        {
            await firsts[writer];
            for (var n = 1; n < Saves; n++)
            {
                await backend.Store.SaveAsync(Saved($"w{writer}", n));
            }
        }))).WaitAsync(Patience);

        for (var writer = 0; writer < Writers; writer++)
        {
            Assert.Equal(Enumerable.Range(0, Saves).Select(n => $"{n:00}"), await Log(backend.Store, $"w{writer}"));
        }

        // Other tests may flush through the same kind of store meanwhile: these saves are at least counted.
        Assert.True(sizes.Sizes.Sum() >= Writers * Saves, $"{sizes.Sizes.Sum()} saves flushed, {Writers * Saves} made");
        Assert.Contains(sizes.Sizes, size => size > 1);
    }

    [Fact]
    public async Task Sqlite_Satisfies_The_Store_Contracts_With_Group_Commit() => await ContractsAsync(Sqlite());

    [PostgresFact]
    public async Task Postgres_Satisfies_The_Store_Contracts_With_Group_Commit() => await ContractsAsync(Postgres("contracts"));

    private static async Task ContractsAsync(Backend backend)
    {
        using var cleanup = backend.Cleanup;
        StoreContracts.CheckpointStoreContract(backend.Store);
        await StoreContracts.FreezeResumeContractAsync(backend.Store);
        await StoreContracts.ConditionalSaveContractAsync(backend.Store);
        await StoreContracts.FreezeEndContractAsync(backend.Store);
        await StoreContracts.LeasedRunContractAsync(backend.Store);
        await (backend.Store switch
        {
            EfCoreCheckpointStore sqlite => StoreContracts.DurableRunContractAsync(sqlite),
            PostgresCheckpointStore postgres => StoreContracts.DurableRunContractAsync(postgres),
            var other => throw new InvalidOperationException($"no durable contract for {other.GetType().Name}"),
        });
    }

    // --- A flush that may have happened ---

    private static NpgsqlException PostgresLost() => new("the connection was lost", new IOException("reset by peer"));

    private static Func<string, ValueTask> Once(string point, Func<Exception> failure)
    {
        var left = 1;
        return reached =>
            reached == point && Interlocked.Decrement(ref left) >= 0 ? throw failure() : ValueTask.CompletedTask;
    }

    [PostgresFact]
    public async Task Postgres_Looks_Up_Each_Row_Of_A_Flush_Whose_Commit_May_Have_Happened()
    {
        var hold = new FlushHold("save:before-commit") { Then = Once("save:after-commit", PostgresLost) };
        var backend = Postgres("in_doubt", hold.Fault, OneAtATime);
        await AmbiguousFlushAsync(backend, hold);
    }

    /// <summary>
    /// The second flush fails after its commit: each row is answered as it would have been, and nothing is
    /// written twice.
    /// </summary>
    private static async Task AmbiguousFlushAsync(Backend backend, FlushHold hold)
    {
        using var cleanup = backend.Cleanup;
        var store = backend.Store;
        await store.SaveAsync(Saved("cas", 0));

        hold.Arm();
        var blocker = store.SaveAsync(Saved("blocker", 0)).AsTask();
        await hold.Held;
        var wins = store.SaveIfLatestAsync(Saved("cas", 1), 0).AsTask();
        var lost = store.SaveIfLatestAsync(Saved("cas", 2), 0).AsTask();
        var plainFirst = store.SaveAsync(Saved("plain", 0)).AsTask();
        var plainSecond = store.SaveAsync(Saved("plain", 1)).AsTask();
        hold.Release();

        await blocker.WaitAsync(Patience);
        Assert.True(await wins.WaitAsync(Patience));
        Assert.False(await lost.WaitAsync(Patience));
        await Task.WhenAll(plainFirst, plainSecond).WaitAsync(Patience);
        Assert.Equal(["00", "01"], await Log(store, "cas"));
        Assert.Equal(["00", "01"], await Log(store, "plain"));
    }

    [PostgresFact]
    public async Task Postgres_Repeats_A_Flush_That_Failed_Before_Its_Commit_And_Writes_Once()
    {
        var backend = Postgres("before_commit", Once("save:before-commit", PostgresLost));
        using var cleanup = backend.Cleanup;

        await backend.Store.SaveAsync(Saved("t", 0));

        Assert.Equal(["00"], await Log(backend.Store, "t"));
    }

    [PostgresFact]
    public async Task Postgres_Fails_Only_The_Row_The_Database_Refuses()
    {
        var hold = new FlushHold("save:before-commit");
        var backend = Postgres("bad_row", hold.Fault, OneAtATime);
        using var cleanup = backend.Cleanup;
        var store = backend.Store;

        hold.Arm();
        var blocker = store.SaveAsync(Saved("blocker", 0)).AsTask();
        await hold.Held;
        var good = store.SaveAsync(Saved("good", 0)).AsTask();

        // JSONB can't hold a NUL character: the database refuses this row, and it alone.
        var bad = store.SaveAsync(Saved("bad", 0) with { FrozenState = JsonNode.Parse("""{"text":"a\u0000b"}""") }).AsTask();
        var alsoGood = store.SaveAsync(Saved("also", 0)).AsTask();
        hold.Release();

        await Task.WhenAll(blocker, good, alsoGood).WaitAsync(Patience);
        await Assert.ThrowsAsync<PostgresException>(() => bad.WaitAsync(Patience));
        Assert.Equal(["00"], await Log(store, "good"));
        Assert.Equal(["00"], await Log(store, "also"));
        Assert.Empty(await Log(store, "bad"));
    }

    [PostgresFact]
    public async Task Disposing_A_Postgres_Store_Flushes_What_Is_Queued()
    {
        var hold = new FlushHold("save:before-commit");
        using var sandbox = new PostgresSandbox("drain");
        var dataSource = sandbox.CreateDataSource();
        var store = PostgresSandbox.Open(dataSource, new PostgresStoreOptions { GroupCommit = OneAtATime, Fault = hold.Fault });

        hold.Arm();
        var blocker = store.SaveAsync(Saved("blocker", 0)).AsTask();
        await hold.Held;
        var queued = store.SaveAsync(Saved("queued", 0)).AsTask();
        var disposed = store.DisposeAsync().AsTask();
        hold.Release();
        await disposed.WaitAsync(Patience);

        // A save's answer reaches its caller on the thread pool (RunContinuationsAsynchronously), a moment
        // after the flush that DisposeAsync waited for: await both rather than look at them at once. A save
        // the dispose dropped would never complete, or would throw.
        await Task.WhenAll(blocker, queued).WaitAsync(Patience);
        using var reader = PostgresSandbox.Open(dataSource);
        Assert.Equal(["00"], await Log(reader, "queued"));
    }

    [Fact]
    public void Without_Group_Commit_A_Store_Saves_As_Before()
    {
        using var store = EfCoreCheckpointStore.CreateInMemory();
        Assert.False(store.GroupCommits);
        using var batched = EfCoreCheckpointStore.CreateInMemory(options: new SqliteStoreOptions { GroupCommit = On });
        Assert.True(batched.GroupCommits);
    }
}
