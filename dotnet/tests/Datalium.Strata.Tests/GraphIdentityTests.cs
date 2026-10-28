using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Schema;
using Microsoft.Extensions.Time.Testing;
using static Datalium.Strata.Tests.StateGraphInterruptionTests;
using Checkpoint = Datalium.Strata.Interruption.Checkpoint;
using CheckpointStatus = Datalium.Strata.Interruption.CheckpointStatus;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/graph_identity.rs</c> (<c>design/31-Graph-Identity.md</c>, SG-13): which
/// graph wrote a thread, through the public API only. Keep the two in step.
/// </summary>
public class GraphIdentityTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
    private static readonly DateTimeOffset Noon = new(2026, 10, 6, 12, 0, 0, TimeSpan.Zero);

    public sealed record Order(
        [property: JsonPropertyName("amount")] int Amount,
        [property: JsonPropertyName("log")] IReadOnlyList<string> Log,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    public sealed record Job(
        [property: JsonPropertyName("n")] int N,
        [property: JsonPropertyName("trail")] IReadOnlyList<string> Trail,
        [property: JsonPropertyName("approved_by")] string? ApprovedBy);

    public sealed record Tick([property: JsonPropertyName("ticks")] int Ticks);

    /// <summary>Every event of every run, in arrival order.</summary>
    private sealed class Collector : IRunObserver
    {
        private readonly ConcurrentQueue<RunEvent> _events = new();

        public void OnEvent(RunEvent runEvent) => _events.Enqueue(runEvent);

        public async Task<List<RunEvent>> After(int runs)
        {
            var deadline = DateTime.UtcNow + Patience;
            while (_events.Count(e => e is RunCompleted) < runs)
            {
                Assert.True(DateTime.UtcNow < deadline, "the runs' events");
                await Task.Delay(5);
            }

            return [.. _events];
        }
    }

    // --- Graphs ---

    private static CompilationOptions Named(string id, string? version, Func<Checkpoint, Checkpoint?>? migrate = null) =>
        new() { GraphId = id, GraphVersion = version, Migrate = migrate };

    /// <summary><c>frozen.json</c>'s graph: <c>PrepareWireTransfer</c> (a boundary) -> <c>ExecuteWireTransfer</c>.</summary>
    private static CompiledGraph<WireTransferState> WireTransfer(CompilationOptions? options = null) =>
        new StateGraph<WireTransferState>()
            .AddNode("PrepareWireTransfer", s => s with { InvoiceGenerated = true })
            .AddInterruptBoundary("PrepareWireTransfer")
            .AddNode("ExecuteWireTransfer", s => s with { IsCompleted = true })
            .AddEdge("PrepareWireTransfer", "ExecuteWireTransfer")
            .Compile(options ?? new CompilationOptions());

    /// <summary>Version 2 of the wire transfer, whose last node is called <c>Send</c>.</summary>
    private static CompiledGraph<WireTransferState> RenamedWireTransfer(CompilationOptions options) =>
        new StateGraph<WireTransferState>()
            .AddNode("PrepareWireTransfer", s => s with { InvoiceGenerated = true })
            .AddInterruptBoundary("PrepareWireTransfer")
            .AddNode("Send", s => s with { IsCompleted = true, ApprovedBy = $"{s.ApprovedBy} via Send" })
            .AddEdge("PrepareWireTransfer", "Send")
            .Compile(options);

    private static CompiledGraph<Job> Approval(CompilationOptions? options = null) =>
        new StateGraph<Job>()
            .AddNode("Draft", j => j with { N = j.N * 2, Trail = [.. j.Trail, "draft"] })
            .AddNode("Approve", j => j with { Trail = [.. j.Trail, "approve"] })
            .AddInterruptBoundary("Approve")
            .AddNode("Send", j => j with { N = j.N + 1, Trail = [.. j.Trail, "send"] })
            .AddEdge("Draft", "Approve")
            .AddEdge("Approve", "Send")
            .Compile(options ?? new CompilationOptions());

    /// <summary><c>nested.json</c>'s graph: <c>Prepare</c> -> <c>Work</c> (<paramref name="child"/>) -> <c>Finish</c>.</summary>
    private static CompiledGraph<Order> Incident(CompiledGraph<Job> child, CompilationOptions? options = null) =>
        new StateGraph<Order>()
            .AddNode("Prepare", o => o with { Log = [.. o.Log, "prepared"] })
            .AddSubgraph(
                "Work",
                child,
                o => new Job(o.Amount, [], null),
                (entry, job) => entry with { Amount = job.N, Log = [.. entry.Log, .. job.Trail], ApprovedBy = job.ApprovedBy })
            .AddNode("Finish", o => o with { Log = [.. o.Log, "finished"] })
            .AddEdge("Prepare", "Work")
            .AddEdge("Work", "Finish")
            .Compile(options ?? new CompilationOptions());

    private static Order NewOrder() => new(5, [], null);

    /// <summary><c>Approve</c> (a boundary) -> <c>Apply</c>, which fails fatally.</summary>
    private static CompiledGraph<WireTransferState> FailingApply(CompilationOptions? options = null) =>
        new StateGraph<WireTransferState>()
            .AddNode("Approve", s => s)
            .AddInterruptBoundary("Approve")
            .AddFallibleNode("Apply", (WireTransferState _) => throw NodeException.Fatal("gateway refused"))
            .AddEdge("Approve", "Apply")
            .Compile(options ?? new CompilationOptions());

    /// <summary><c>Check</c> -> <c>Window</c> (a five-minute timer) -> <c>Done</c>.</summary>
    private static CompiledGraph<Tick> Ticker(CompilationOptions? options = null) =>
        new StateGraph<Tick>()
            .AddNode("Check", t => t with { Ticks = t.Ticks + 1 })
            .AddTimer("Window", TimeSpan.FromMinutes(5))
            .AddNode("Done", t => t)
            .AddEdge("Check", "Window")
            .AddEdge("Window", "Done")
            .Compile(options ?? new CompilationOptions());

    /// <summary><c>Start</c> -> <c>Deploying</c> (waits for <c>deployment.finished</c>) -> <c>Done</c>.</summary>
    private static CompiledGraph<Tick> Deployer(CompilationOptions? options = null) =>
        new StateGraph<Tick>()
            .AddNode("Start", t => t)
            .AddWaitForEvent("Deploying", "deployment.finished")
            .AddNode("Done", t => t with { Ticks = t.Ticks + 1 })
            .AddEdge("Start", "Deploying")
            .AddEdge("Deploying", "Done")
            .Compile(options ?? new CompilationOptions());

    // --- Helpers ---

    private static JsonNode ReadFixture(string relative, [CallerFilePath] string sourceFilePath = "") =>
        JsonNode.Parse(File.ReadAllText(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "schema", "fixtures", relative))))!;

    private static Checkpoint WithoutTimestamps(Checkpoint checkpoint) =>
        checkpoint with
        {
            CreatedAt = string.Empty,
            Nested = checkpoint.Nested is null ? null : WithoutTimestamps(checkpoint.Nested),
        };

    private static void AssertMatchesFixture(string name, Checkpoint actual)
    {
        var expected = WithoutTimestamps(Checkpoint.FromWireJson(ReadFixture(Path.Combine("checkpoint-lifecycle", name)))).ToWireJson();
        var got = WithoutTimestamps(actual).ToWireJson();
        Assert.True(JsonNode.DeepEquals(expected, got), got.ToJsonString());
    }

    private static string Refused<T>(GraphExecutionResult<T> result) =>
        Assert.IsAssignableFrom<GraphExecutionResult<T>.Failed>(result).Message;

    private static T Completed<T>(GraphExecutionResult<T> result)
    {
        if (result is GraphExecutionResult<T>.Failed failed)
        {
            Assert.Fail($"expected the run to complete, got: {failed.Message}");
        }

        return Assert.IsType<GraphExecutionResult<T>.Completed>(result).State;
    }

    private static async Task<List<Checkpoint>> LogOf(InMemoryCheckpointStore store, string threadId) =>
        [.. (await store.GetLogAsync(threadId)).Entries.Select(e => e.Checkpoint)];

    // --- What is written ---

    [Fact]
    public async Task A_Named_Graph_Writes_Its_Identity_And_Matches_The_Shared_Fixture()
    {
        var store = new InMemoryCheckpointStore();
        var graph = WireTransfer(Named("wire-transfer", "2"));

        Assert.IsType<GraphExecutionResult<WireTransferState>.Interrupted>(
            await graph.RunAsync("thread_hitl_demo", InitialState(), store));

        var checkpoint = store.Load("thread_hitl_demo")!;
        Assert.Equal(new GraphIdentity("wire-transfer", "2"), checkpoint.Graph);
        Assert.Equal(CheckpointSchema.GraphVersion, checkpoint.SchemaVersion);
        AssertMatchesFixture("identified.json", checkpoint);
        Assert.Equal(new GraphIdentity("wire-transfer", "2"), graph.Identity);
    }

    [Fact]
    public async Task Every_Level_Carries_Its_Own_Graph_And_Matches_The_Nested_Fixture()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Incident(Approval(Named("approval", "3")), Named("incident", "1"));

        await graph.RunAsync("thread_nested_demo", NewOrder(), store);

        var checkpoint = store.Load("thread_nested_demo")!;
        Assert.Equal(new GraphIdentity("incident", "1"), checkpoint.Graph);
        Assert.Equal(new GraphIdentity("approval", "3"), checkpoint.Nested!.Graph);
        AssertMatchesFixture("nested-identified.json", checkpoint);
    }

    [Fact]
    public async Task A_Child_Without_An_Identity_Writes_None_On_Its_Level()
    {
        var store = new InMemoryCheckpointStore();
        await Incident(Approval(), Named("incident", null)).RunAsync("t", NewOrder(), store);

        var checkpoint = store.Load("t")!;
        Assert.Equal(new GraphIdentity("incident", null), checkpoint.Graph);
        Assert.Null(checkpoint.Nested!.Graph);
        Assert.Equal(CheckpointSchema.GraphVersion, checkpoint.SchemaVersion);
        Assert.False(checkpoint.Nested.ToWireJson().ContainsKey("graph"));
        Assert.Null(checkpoint.ToWireJson()["graph"]!["version"]);
    }

    [Fact]
    public async Task Failed_Cancelled_And_Hop_Limited_Checkpoints_Carry_It()
    {
        var store = new InMemoryCheckpointStore();
        var identity = new GraphIdentity("payments", "7");

        var failing = FailingApply(Named("payments", "7"));
        await failing.RunAsync("failed", InitialState(), store);
        Refused(await failing.ResumeAsync("failed", HumanSignal.Approve("alice"), store));
        Assert.Equal(CheckpointStatus.Failed, store.Load("failed")!.Status);
        Assert.Equal(identity, store.Load("failed")!.Graph);

        var cancelled = await WireTransfer(Named("payments", "7"))
            .RunAsync("cancelled", InitialState(), store, new CancellationToken(canceled: true));
        Assert.IsType<GraphExecutionResult<WireTransferState>.Cancelled>(cancelled);
        Assert.Equal(identity, store.Load("cancelled")!.Graph);

        var limited = new StateGraph<WireTransferState>()
            .AddNode("A", s => s)
            .AddNode("B", s => s)
            .AddEdge("A", "B")
            .Compile(new CompilationOptions { GraphId = "payments", GraphVersion = "7", MaxNodeHops = 1 });
        Assert.IsType<GraphExecutionResult<WireTransferState>.HopLimitExceeded>(
            await limited.RunAsync("limited", InitialState(), store));
        Assert.Equal(identity, store.Load("limited")!.Graph);
    }

    [Fact]
    public async Task An_Unnamed_Graph_Writes_No_Graph_And_Keeps_Its_Version()
    {
        var store = new InMemoryCheckpointStore();
        var graph = WireTransfer();

        await graph.RunAsync("thread_hitl_demo", InitialState(), store);

        var checkpoint = store.Load("thread_hitl_demo")!;
        Assert.Null(graph.Identity);
        Assert.Null(checkpoint.Graph);
        Assert.Equal(CheckpointSchema.Version, checkpoint.SchemaVersion);
        Assert.False(checkpoint.ToWireJson().ContainsKey("graph"));
    }

    [Fact]
    public async Task Ending_Keeps_Every_Levels_Graph()
    {
        var store = new InMemoryCheckpointStore();
        var graph = Incident(Approval(Named("approval", "3")), Named("incident", "1"));
        await graph.RunAsync("t", NewOrder(), store);

        Assert.IsType<GraphExecutionResult<Order>.Cancelled>(await graph.EndAsync("t", store));

        var ended = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Ended, ended.Status);
        Assert.Equal(CheckpointSchema.GraphVersion, ended.SchemaVersion);
        Assert.Equal(new GraphIdentity("incident", "1"), ended.Graph);
        Assert.Equal(new GraphIdentity("approval", "3"), ended.Nested!.Graph);
    }

    // --- Refusals ---

    [Fact]
    public async Task A_Resume_Under_Another_Id_Is_Refused_And_Writes_Nothing()
    {
        var store = new InMemoryCheckpointStore();
        await WireTransfer(Named("wire-transfer", "2")).RunAsync("t", InitialState(), store);
        var log = await LogOf(store, "t");

        var message = Refused(await WireTransfer(Named("payroll", "2")).ResumeAsync("t", HumanSignal.Approve("alice"), store));

        Assert.Equal("thread 't' was written by graph 'wire-transfer' version 2, not 'payroll' version 2", message);
        Assert.Equal(log, await LogOf(store, "t"));

        // The right graph still resumes it.
        Assert.True(Completed(await WireTransfer(Named("wire-transfer", "2")).ResumeAsync("t", HumanSignal.Approve("alice"), store)).IsCompleted);
    }

    [Fact]
    public async Task A_Resume_Under_Another_Version_Without_A_Migration_Is_Refused()
    {
        var store = new InMemoryCheckpointStore();
        await WireTransfer(Named("wire-transfer", "2")).RunAsync("t", InitialState(), store);
        var log = await LogOf(store, "t");

        var message = Refused(await WireTransfer(Named("wire-transfer", "3")).ResumeAsync("t", HumanSignal.Approve("alice"), store));

        Assert.Equal(
            "thread 't' was written by graph 'wire-transfer' version 2, not version 3, and this graph has no migration",
            message);
        Assert.Equal(log, await LogOf(store, "t"));

        message = Refused(await WireTransfer(Named("wire-transfer", null)).ResumeAsync("t", HumanSignal.Approve("alice"), store));
        Assert.Equal(
            "thread 't' was written by graph 'wire-transfer' version 2, not unversioned, and this graph has no migration",
            message);
    }

    [Fact]
    public async Task A_Graph_Without_An_Identity_Can_Not_Resume_A_Named_Thread()
    {
        var store = new InMemoryCheckpointStore();
        await WireTransfer(Named("wire-transfer", "2")).RunAsync("t", InitialState(), store);

        var message = Refused(await WireTransfer().ResumeAsync("t", HumanSignal.Approve("alice"), store));

        Assert.Equal("thread 't' was written by graph 'wire-transfer' version 2, and this graph has no identity", message);
    }

    [Fact]
    public async Task A_Nested_Level_Is_Checked_By_Its_Own_Graph()
    {
        var store = new InMemoryCheckpointStore();
        await Incident(Approval(Named("approval", "3")), Named("incident", "1")).RunAsync("t", NewOrder(), store);
        var log = await LogOf(store, "t");

        var message = Refused(await Incident(Approval(Named("approval", "4")), Named("incident", "1"))
            .ResumeAsync("t", HumanSignal.Approve("alice"), store));

        Assert.Equal(
            "thread 't' at level [Work] was written by graph 'approval' version 3, not version 4, and this graph has no migration",
            message);
        Assert.Equal(log, await LogOf(store, "t"));
    }

    [Fact]
    public async Task End_And_Signal_Under_Another_Graph_Are_Refused()
    {
        var store = new InMemoryCheckpointStore();
        await WireTransfer(Named("wire-transfer", "2")).RunAsync("t", InitialState(), store);
        await Deployer(Named("deployer", "1")).RunAsync("d", new Tick(0), store);
        var (transfers, deploys) = (await LogOf(store, "t"), await LogOf(store, "d"));

        Assert.Equal(
            "thread 't' was written by graph 'wire-transfer' version 2, not 'payroll' version 2",
            Refused(await WireTransfer(Named("payroll", "2")).EndAsync("t", store)));
        Assert.Equal(
            "thread 'd' was written by graph 'deployer' version 1, not 'other' version 1",
            Refused(await Deployer(Named("other", "1")).SignalAsync("d", new EventSignal("deployment.finished"), store)));
        Assert.Equal(transfers, await LogOf(store, "t"));
        Assert.Equal(deploys, await LogOf(store, "d"));

        // Its own graph does both.
        Assert.Equal(1, Completed(await Deployer(Named("deployer", "1")).SignalAsync("d", new EventSignal("deployment.finished"), store)).Ticks);
        Assert.IsType<GraphExecutionResult<WireTransferState>.Cancelled>(await WireTransfer(Named("wire-transfer", "2")).EndAsync("t", store));
    }

    // --- Migration ---

    [Fact]
    public async Task A_Migration_Takes_Another_Versions_Thread_And_The_Stamp_Records_The_New_Identity()
    {
        var store = new InMemoryCheckpointStore();
        await WireTransfer(Named("wire-transfer", "1")).RunAsync("t", InitialState(), store);
        var seen = new List<GraphIdentity?>();
        var v2 = RenamedWireTransfer(Named("wire-transfer", "2", checkpoint =>
        {
            seen.Add(checkpoint.Graph);
            if (checkpoint.Graph?.Version != "1")
            {
                return null;
            }

            var state = checkpoint.FrozenState!.DeepClone().AsObject();
            state["currency"] = "EUR";
            return checkpoint with
            {
                NextNode = checkpoint.NextNode == "ExecuteWireTransfer" ? "Send" : checkpoint.NextNode,
                FrozenState = state,
                ThreadId = "ignored: the engine keeps the stored thread id",
            };
        }));

        var state = Completed(await v2.ResumeAsync("t", HumanSignal.Approve("alice").WithOverride("approved_by", "alice"), store));

        Assert.Equal("EUR", state.Currency);
        Assert.Equal("alice via Send", state.ApprovedBy);
        Assert.Equal([new GraphIdentity("wire-transfer", "1")], seen);
        var log = await LogOf(store, "t");
        Assert.Equal(2, log.Count);
        var stamp = log[1];
        Assert.Equal("t", stamp.ThreadId);
        Assert.NotNull(stamp.ResumedAt);
        Assert.Equal("Send", stamp.NextNode);
        Assert.Equal(new GraphIdentity("wire-transfer", "2"), stamp.Graph);
        Assert.Equal(CheckpointSchema.GraphVersion, stamp.SchemaVersion);
        Assert.Equal(new GraphIdentity("wire-transfer", "1"), log[0].Graph);
    }

    [Fact]
    public async Task A_Migration_That_Refuses_Leaves_The_Thread_Resumable()
    {
        var store = new InMemoryCheckpointStore();
        await WireTransfer(Named("wire-transfer", "1")).RunAsync("t", InitialState(), store);
        var log = await LogOf(store, "t");

        var message = Refused(await WireTransfer(Named("wire-transfer", "2", _ => null))
            .ResumeAsync("t", HumanSignal.Approve("alice"), store));

        Assert.Equal(
            "thread 't' was written by graph 'wire-transfer' version 1, not version 2, and this graph's migration refused it",
            message);
        Assert.Equal(log, await LogOf(store, "t"));
        Assert.True(Completed(await WireTransfer(Named("wire-transfer", "1")).ResumeAsync("t", HumanSignal.Approve("alice"), store)).IsCompleted);
    }

    [Fact]
    public async Task A_Nested_Level_Is_Migrated_By_Its_Own_Graph_Once()
    {
        var store = new InMemoryCheckpointStore();
        await Incident(Approval(Named("approval", "3")), Named("incident", "1")).RunAsync("t", NewOrder(), store);
        var calls = 0;
        var child = Approval(Named("approval", "4", checkpoint =>
        {
            Interlocked.Increment(ref calls);
            return checkpoint;
        }));

        var order = Completed(await Incident(child, Named("incident", "1")).ResumeAsync("t", HumanSignal.Approve("alice"), store));

        Assert.Equal(["prepared", "draft", "approve", "send", "finished"], order.Log);
        Assert.Equal(1, calls);
        var stamp = (await LogOf(store, "t"))[1];
        Assert.Equal(new GraphIdentity("incident", "1"), stamp.Graph);
        Assert.Equal(new GraphIdentity("approval", "4"), stamp.Nested!.Graph);
    }

    [Fact]
    public async Task An_Unknown_Thread_Resumes_Under_A_Named_Graph_Which_Then_Writes_Its_Identity()
    {
        var store = new InMemoryCheckpointStore();

        // frozen.json predates identities: 1.0.0, no graph.
        var unknown = Checkpoint.FromWireJson(ReadFixture(Path.Combine("checkpoint-lifecycle", "frozen.json")));
        Assert.Null(unknown.Graph);
        store.Save(unknown);
        Assert.True(Completed(await WireTransfer(Named("wire-transfer", "2"))
            .ResumeAsync(unknown.ThreadId, HumanSignal.Approve("alice"), store)).IsCompleted);

        // A failure after an unknown checkpoint is written with the resuming graph's identity.
        var failing = FailingApply(Named("payments", "7"));
        store.Save(unknown with { ThreadId = "f", SuspendedNode = "Approve", NextNode = "Apply" });
        Refused(await failing.ResumeAsync("f", HumanSignal.Approve("alice"), store));
        var log = await LogOf(store, "f");
        Assert.Null(log[1].Graph);
        Assert.Equal(new GraphIdentity("payments", "7"), log[2].Graph);
    }

    // --- Owns ---

    [Fact]
    public void Owns_Says_Which_Threads_A_Graph_Takes()
    {
        var unknown = Checkpoint.FromWireJson(ReadFixture(Path.Combine("checkpoint-lifecycle", "frozen.json")));
        var v2 = unknown with { Graph = new GraphIdentity("wire-transfer", "2") };

        Assert.True(WireTransfer().Owns(unknown));
        Assert.False(WireTransfer().Owns(v2));
        Assert.True(WireTransfer(Named("wire-transfer", "2")).Owns(unknown));
        Assert.True(WireTransfer(Named("wire-transfer", "2")).Owns(v2));
        Assert.False(WireTransfer(Named("payroll", "2")).Owns(v2));
        Assert.False(WireTransfer(Named("wire-transfer", "3")).Owns(v2));
        Assert.True(WireTransfer(Named("wire-transfer", "3", c => c)).Owns(v2));
    }

    // --- Compiling ---

    [Fact]
    public void Compile_Refuses_A_Blank_Identity_Or_Parts_Without_An_Id()
    {
        var builder = new StateGraph<Tick>().AddNode("A", t => t);

        Assert.Equal(
            "GraphId must not be blank (Parameter 'options')",
            Assert.Throws<ArgumentException>(() => builder.Compile(new CompilationOptions { GraphId = " " })).Message);
        Assert.Equal(
            "GraphVersion must not be blank (Parameter 'options')",
            Assert.Throws<ArgumentException>(() => builder.Compile(new CompilationOptions { GraphId = "a", GraphVersion = "" })).Message);
        Assert.Equal(
            "GraphVersion needs a GraphId (Parameter 'options')",
            Assert.Throws<ArgumentException>(() => builder.Compile(new CompilationOptions { GraphVersion = "1" })).Message);
        Assert.Equal(
            "Migrate needs a GraphId (Parameter 'options')",
            Assert.Throws<ArgumentException>(() => builder.Compile(new CompilationOptions { Migrate = c => c })).Message);
    }

    // --- The drivers ---

    [Fact]
    public async Task WakeDueAsync_Lists_Only_Its_Own_Graphs_Threads()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var run = new RunOptions { TimeProvider = clock };
        var a = Ticker(Named("ticker-a", "1"));
        var b = Ticker(Named("ticker-b", "1"));
        await a.RunAsync("a", new Tick(0), store, run);
        await b.RunAsync("b", new Tick(0), store, run);
        await Ticker().RunAsync("unknown", new Tick(0), store, run);
        clock.Advance(TimeSpan.FromMinutes(6));

        // Same node names, same waits: only identity tells them apart, and the listing already does.
        var report = await a.WakeDueAsync(store, new WakeOptions { Run = run });
        Assert.Equal(["a"], report.Woken.Select(w => w.ThreadId));
        Assert.Empty(report.Skipped);

        // Without the match, the unknown thread is woken and the other graph's skipped, with why.
        report = await a.WakeDueAsync(store, new WakeOptions { Run = run, MatchGraph = false });
        Assert.Equal(["unknown"], report.Woken.Select(w => w.ThreadId));
        var skipped = Assert.Single(report.Skipped);
        Assert.Equal("b", skipped.ThreadId);
        Assert.Equal("thread 'b' was written by graph 'ticker-b' version 1, not 'ticker-a' version 1", skipped.Reason);
    }

    [Fact]
    public async Task WakeDueAsync_With_A_Migration_Wakes_An_Older_Versions_Thread()
    {
        var store = new InMemoryCheckpointStore();
        var clock = new FakeTimeProvider(Noon);
        var run = new RunOptions { TimeProvider = clock };
        await Ticker(Named("ticker", "1")).RunAsync("old", new Tick(0), store, run);
        clock.Advance(TimeSpan.FromMinutes(6));

        Assert.Empty((await Ticker(Named("ticker", "2")).WakeDueAsync(store, new WakeOptions { Run = run })).Woken);

        var report = await Ticker(Named("ticker", "2", c => c)).WakeDueAsync(store, new WakeOptions { Run = run });
        var woken = Assert.Single(report.Woken);
        Assert.Equal("old", woken.ThreadId);
        Assert.Equal(1, Completed(woken.Result).Ticks);
    }

    // --- Run events and telemetry ---

    [Fact]
    public async Task RunStarted_Carries_The_Graph_And_The_Run_Span_Is_Tagged()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var store = new InMemoryCheckpointStore();
        var collector = new Collector();
        var threadId = $"span-{Guid.NewGuid():N}";

        await WireTransfer(Named("wire-transfer", "2")).RunAsync(threadId, InitialState(), store, new RunOptions { Observer = collector });
        await WireTransfer().RunAsync("unnamed-" + threadId, InitialState(), store, new RunOptions { Observer = collector });

        var started = (await collector.After(2)).OfType<RunStarted>().ToList();
        Assert.Equal(new GraphIdentity("wire-transfer", "2"), started.Single(e => e.ThreadId == threadId).Graph);
        Assert.Null(started.Single(e => e.ThreadId != threadId).Graph);
        var run = spans.Single(s => s.OperationName == "strata.run" && (string?)s.GetTagItem("strata.thread_id") == threadId);
        Assert.Equal("wire-transfer", run.GetTagItem("strata.graph.id"));
        Assert.Equal("2", run.GetTagItem("strata.graph.version"));
    }

    // --- The query port ---

    [Fact]
    public async Task A_Query_With_A_Version_And_No_Id_Is_Refused()
    {
        var store = new InMemoryCheckpointStore();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await store.ListThreadsAsync(new ThreadQuery { GraphVersion = "2" }));
    }

    [Fact]
    public async Task A_Summary_Says_Which_Graph_Wrote_The_Thread()
    {
        var store = new InMemoryCheckpointStore();
        await Incident(Approval(Named("approval", "3")), Named("incident", "1")).RunAsync("t", NewOrder(), store);

        var summary = Assert.Single((await store.ListThreadsAsync(new ThreadQuery { GraphId = "incident" })).Threads);

        Assert.Equal(new GraphIdentity("incident", "1"), summary.Graph);
        Assert.Empty((await store.ListThreadsAsync(new ThreadQuery { GraphId = "approval" })).Threads);
    }

    // --- Definitions ---

    private static CompiledGraph<Tick> IdentityFixtureGraph()
    {
        var check = new StateGraph<Tick>().AddNode("Probe", t => t).Compile();
        var remediation = new StateGraph<Tick>()
            .AddNode("Apply", t => t)
            .AddSubgraph("Check", check, t => t, (_, t) => t)
            .AddEdge("Apply", "Check")
            .Compile(Named("remediation", null));
        return new StateGraph<Tick>()
            .AddNode("Plan", t => t)
            .AddSubgraph("Remediate", remediation, t => t, (_, t) => t)
            .AddNode("Report", t => t)
            .AddEdge("Plan", "Remediate")
            .AddEdge("Remediate", "Report")
            .Compile(Named("incident", "1"));
    }

    [Fact]
    public void A_Compiled_Graph_Exports_Every_Levels_Identity_As_The_Shared_Fixture()
    {
        var exported = JsonSerializer.SerializeToNode(IdentityFixtureGraph().ToDefinition());

        Assert.True(JsonNode.DeepEquals(ReadFixture(Path.Combine("graph-export", "identity.json")), exported), exported!.ToJsonString());
    }

    [Fact]
    public void The_Identity_Fixture_Round_Trips_And_Its_Shape_Is_Checked()
    {
        var fixture = ReadFixture(Path.Combine("graph-export", "identity.json"));
        var definition = fixture.Deserialize<GraphDefinition>()!;

        var topology = definition.ToTopology();
        Assert.Equal(new GraphIdentity("incident", "1"), topology.Identity);
        Assert.Equal(new GraphIdentity("remediation", null), topology.DetailOf("Remediate")!.Graph!.Identity);
        Assert.True(JsonNode.DeepEquals(fixture, JsonSerializer.SerializeToNode(topology.ToDefinition())));

        // The builder has no identity: it is given at compile time.
        Assert.Null(new StateGraph<Tick>().AddNode("A", t => t).ToDefinition().Identity);

        var old = fixture.DeepClone();
        old["schema_version"] = "1.4.0";
        Assert.Equal(
            "the definition has an identity, which schema_version 1.4.0 doesn't have",
            Assert.Throws<FormatException>(() => old.Deserialize<GraphDefinition>()!.ToTopology()).Message);
        var blank = fixture.DeepClone();
        blank["nodes"]![1]!["graph"]!["identity"]!["id"] = "";
        Assert.Equal(
            "the definition (in 'Remediate') has an identity with an empty id",
            Assert.Throws<FormatException>(() => blank.Deserialize<GraphDefinition>()!.ToTopology()).Message);
    }
}
