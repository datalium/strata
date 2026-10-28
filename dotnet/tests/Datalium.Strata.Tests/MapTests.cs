using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Streaming;
using Datalium.Strata.Validation;
using Microsoft.Extensions.Time.Testing;
using CheckpointStatus = Datalium.Strata.Interruption.CheckpointStatus;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/map.rs</c> (<c>design/27-Map-Fan-Out.md</c>, SG-06): a map runs a
/// compiled child graph per item of a list read from the state, with keys checked before anything
/// starts, items named <c>&lt;map&gt;/&lt;key&gt;</c>, <c>MapExpanded</c>, the group runner's limits,
/// timeouts, joins and partial results, items that wind down, and <c>kind: map</c> in the exported
/// definition (<c>schema/fixtures/graph-export/map.json</c>,
/// <c>graph-validation/map-template-errors.json</c>). Keep the two in step.
/// </summary>
public class MapTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    /// <summary>The parent's state: the items to fan out over, and what the reducer found.</summary>
    public sealed record Case(IReadOnlyList<string> Items, IReadOnlyList<string> Found);

    /// <summary>An item's child state: the item, and the child nodes it passed through.</summary>
    public sealed record Probe(string Item, IReadOnlyList<string> Trail);

    private static Case NewCase(params string[] items) => new(items, []);

    private static Probe Passed(Probe probe, string node) => probe with { Trail = [.. probe.Trail, node] };

    private static ValueTask<Probe> Assessed(Probe probe) => ValueTask.FromResult(Passed(probe, "assess"));

    /// <summary>A child graph: <c>assess</c> (fallible, <paramref name="assess"/>), then <c>summarize</c>.</summary>
    private static CompiledGraph<Probe> Child(
        Func<Probe, NodeContext, CancellationToken, ValueTask<Probe>>? assess = null,
        CompilationOptions? options = null) =>
        new StateGraph<Probe>()
            .AddFallibleNode("assess", assess ?? ((probe, _, _) => Assessed(probe)))
            .AddNode("summarize", probe => Passed(probe, "summarize"))
            .AddEdge("assess", "summarize")
            .Compile(options ?? new CompilationOptions());

    /// <summary>
    /// A graph whose one node is the map <c>Investigate</c> over the case's items, keyed by themselves;
    /// the reducer records each succeeded item's trail as <c>item:node+node</c> and keeps the outcomes.
    /// </summary>
    private static StateGraph<Case> Mapping(
        CompiledGraph<Probe> child,
        MapOptions<Probe>? options = null,
        ConcurrentQueue<BranchOutcomes<Probe>>? seen = null) =>
        new StateGraph<Case>()
            .AddMapWithOutcomes(
                "Investigate",
                c => c.Items,
                item => item,
                child,
                (_, item) => new Probe(item, []),
                (c, results) =>
                {
                    seen?.Enqueue(results);
                    return c with { Found = [.. results.Succeeded.Select(p => $"{p.Item}:{string.Join('+', p.Trail)}")] };
                },
                options);

    private static async Task<List<RunEvent>> Drain(RunEventChannel channel)
    {
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        return events;
    }

    /// <summary>
    /// Runs <paramref name="graph"/> on <paramref name="start"/>, advancing <paramref name="clock"/> a
    /// fifth of a second at a time until it ends. The clock moves whether or not the run has set its timers yet,
    /// so a small step keeps a slow machine from letting much fake time pass before a timer is set.
    /// </summary>
    private static async Task<(GraphExecutionResult<Case> Result, List<RunEvent> Events)> Run(
        CompiledGraph<Case> graph,
        Case start,
        FakeTimeProvider? clock = null,
        ICheckpointStore? store = null,
        CancellationToken cancellationToken = default)
    {
        var channel = new RunEventChannel();
        var task = graph.RunAsync(
            "t", start, store ?? new InMemoryCheckpointStore(), new RunOptions { Observer = channel, TimeProvider = clock }, cancellationToken);
        for (var i = 0; clock is not null && i < 100_000 && !task.IsCompleted; i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(200));
            await Task.Delay(1);
        }

        var result = await task.WaitAsync(Patience);
        return (result, await Drain(channel));
    }

    private static Case Completed(GraphExecutionResult<Case> result) =>
        Assert.IsType<GraphExecutionResult<Case>.Completed>(result).State;

    /// <summary>The events that tell a map's story, one line each, in order.</summary>
    private static List<string> Story(IEnumerable<RunEvent> events) =>
    [
        .. events.Select(e => e switch
        {
            NodeStarted => $"node {e.NodePath}",
            NodeCompleted c => $"node done {e.NodePath} {c.Result}",
            MapExpanded m => $"expanded {e.NodePath} [{string.Join(", ", m.Keys)}]",
            BranchStarted => $"item {e.NodePath}",
            BranchCompleted b => $"item done {e.NodePath} {b.Result}" + (b.Reason is { } reason ? $" {reason}" : string.Empty),
            JoinDecided j => $"decided {e.NodePath} {j.Policy} {(j.Met ? "met" : "unmet")} [{string.Join(", ", j.Deciding)}]",
            _ => null,
        }).OfType<string>(),
    ];

    private static int IndexOf(List<string> story, string line)
    {
        var index = story.IndexOf(line);
        Assert.True(index >= 0, $"'{line}' is not in the story:\n{string.Join('\n', story)}");
        return index;
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value
               && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }

    // --- Items -------------------------------------------------------------------------

    [Fact]
    public async Task An_Empty_List_Reduces_With_No_Results()
    {
        var seen = new ConcurrentQueue<BranchOutcomes<Probe>>();
        var (result, events) = await Run(Mapping(Child(), seen: seen).Compile(), NewCase());

        Assert.Empty(Completed(result).Found);
        var outcomes = Assert.Single(seen);
        Assert.Empty(outcomes);
        Assert.True(outcomes.JoinMet);
        Assert.Equal(["node Investigate", "expanded Investigate []", "node done Investigate Committed"], Story(events));
    }

    [Fact]
    public async Task One_Item_Runs_Its_Child()
    {
        var (result, events) = await Run(Mapping(Child()).Compile(), NewCase("h1"));

        Assert.Equal(["h1:assess+summarize"], Completed(result).Found);
        Assert.Equal(
            [
                "node Investigate",
                "expanded Investigate [h1]",
                "item Investigate/h1",
                "node Investigate/h1/assess",
                "node done Investigate/h1/assess Committed",
                "node Investigate/h1/summarize",
                "node done Investigate/h1/summarize Committed",
                "item done Investigate/h1 Succeeded",
                "node done Investigate Committed",
            ],
            Story(events));
    }

    [Fact]
    public async Task Many_Items_Respect_The_Limit_Start_In_List_Order_And_Reduce_In_List_Order()
    {
        var inFlight = 0;
        var peak = 0;
        var started = new ConcurrentQueue<string>();
        var child = Child(async (probe, _, ct) =>
        {
            started.Enqueue(probe.Item);
            InterlockedMax(ref peak, Interlocked.Increment(ref inFlight));

            // Later items finish first, so completion order isn't list order.
            await Task.Delay(probe.Item == "a" ? 60 : 20, ct);
            Interlocked.Decrement(ref inFlight);
            return Passed(probe, "assess");
        });

        var (result, _) = await Run(
            Mapping(child, new MapOptions<Probe> { MaxConcurrency = 2 }).Compile(), NewCase("a", "b", "c", "d", "e"));

        Assert.Equal(
            ["a:assess+summarize", "b:assess+summarize", "c:assess+summarize", "d:assess+summarize", "e:assess+summarize"],
            Completed(result).Found);
        Assert.Equal(2, peak);
        Assert.Equal(["a", "b", "c", "d", "e"], started);
    }

    [Fact]
    public async Task The_Default_Concurrency_Limit_Is_Eight()
    {
        var inFlight = 0;
        var peak = 0;
        var child = Child(async (probe, _, ct) =>
        {
            InterlockedMax(ref peak, Interlocked.Increment(ref inFlight));
            await Task.Delay(30, ct);
            Interlocked.Decrement(ref inFlight);
            return Passed(probe, "assess");
        });
        var items = Enumerable.Range(1, 12).Select(i => $"h{i}").ToArray();

        var (result, _) = await Run(Mapping(child).Compile(), NewCase(items));

        Assert.Equal(12, Completed(result).Found.Count);
        Assert.Equal(8, MapOptions<Probe>.DefaultMaxConcurrency);
        Assert.Equal(8, peak);
    }

    [Fact]
    public async Task The_Plain_Reducer_Gets_The_Childrens_States_In_List_Order()
    {
        var graph = new StateGraph<Case>()
            .AddMap(
                "Investigate",
                c => c.Items,
                item => item,
                Child(),
                (_, item) => new Probe(item, []),
                (c, probes) => c with { Found = [.. probes.Select(p => p.Item)] })
            .Compile();

        var (result, _) = await Run(graph, NewCase("x", "y"));

        Assert.Equal(["x", "y"], Completed(result).Found);
    }

    // --- Keys --------------------------------------------------------------------------

    [Theory]
    [InlineData(new[] { "h1", "h2", "h1" }, "map 'Investigate': items 0 and 2 have the same key 'h1'")]
    [InlineData(new[] { "h1", "" }, "map 'Investigate': item 1 has an empty key")]
    [InlineData(new[] { "a/b" }, "map 'Investigate': item 0 has the key 'a/b', which contains '/'")]
    public async Task Bad_Keys_Fail_The_Map_Fatally_Before_Anything_Starts(string[] items, string message)
    {
        var ran = 0;
        var child = Child((probe, _, _) =>
        {
            Interlocked.Increment(ref ran);
            return Assessed(probe);
        });
        var graph = Mapping(child).SetRecoveryPolicy("Investigate", RecoveryPolicy.Retry(3)).Compile();

        var (result, events) = await Run(graph, NewCase(items));

        var failed = Assert.IsType<GraphExecutionResult<Case>.NodeFailed>(result);
        Assert.Equal(("Investigate", 1, message), (failed.Node, failed.Attempts, failed.Message));
        Assert.Equal(0, ran);
        Assert.DoesNotContain(events, e => e is MapExpanded or BranchStarted);
    }

    [Fact]
    public async Task More_Items_Than_MaxItems_Fail_The_Map_Fatally()
    {
        var graph = Mapping(Child(), new MapOptions<Probe> { MaxItems = 2 })
            .SetRecoveryPolicy("Investigate", RecoveryPolicy.Retry(3))
            .Compile();

        var (result, events) = await Run(graph, NewCase("a", "b", "c"));

        var failed = Assert.IsType<GraphExecutionResult<Case>.NodeFailed>(result);
        Assert.Equal((1, "map 'Investigate' has 3 items, more than its limit of 2"), (failed.Attempts, failed.Message));
        Assert.DoesNotContain(events, e => e is MapExpanded or BranchStarted);
    }

    [Fact]
    public void Options_Refuse_Values_Out_Of_Range()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new MapOptions<Probe> { MaxConcurrency = 0 });
        Assert.Throws<ArgumentOutOfRangeException>(() => new MapOptions<Probe> { ItemTimeout = TimeSpan.Zero });
        Assert.Throws<ArgumentOutOfRangeException>(() => new MapOptions<Probe> { MaxItems = 0 });
    }

    // --- Failures, partial results, timeouts, joins ---------------------------------------

    [Fact]
    public async Task KeepSucceeded_Hands_Every_Outcome_To_The_Reducer()
    {
        var seen = new ConcurrentQueue<BranchOutcomes<Probe>>();
        var child = Child((probe, _, _) => probe.Item == "h2"
            ? ValueTask.FromException<Probe>(NodeException.Fatal("no evidence"))
            : Assessed(probe));

        var (result, events) = await Run(
            Mapping(child, new MapOptions<Probe> { Partial = PartialResults.KeepSucceeded }, seen).Compile(),
            NewCase("h1", "h2", "h3"));

        Assert.Equal(["h1:assess+summarize", "h3:assess+summarize"], Completed(result).Found);
        var outcomes = Assert.Single(seen);
        Assert.Equal(
            [("h1", BranchStatus.Succeeded), ("h2", BranchStatus.Failed), ("h3", BranchStatus.Succeeded)],
            outcomes.Select(o => (o.Name, o.Status)));
        Assert.Equal("failed at 'assess' after 1 attempt(s): no evidence", outcomes["h2"]!.Error!.Message);
        Assert.Equal(NodeFailureKind.Fatal, outcomes["h2"]!.Error!.Kind);
        Assert.False(outcomes.JoinMet);
        Assert.Contains("item done Investigate/h2 Failed", Story(events));
    }

    [Fact]
    public async Task A_Transient_Child_Failure_Fails_The_Map_Transiently_With_Its_Hint()
    {
        var store = new InMemoryCheckpointStore();
        var child = Child((probe, _, _) => probe.Item == "b"
            ? ValueTask.FromException<Probe>(NodeException.Transient("rate limited", TimeSpan.FromSeconds(5)))
            : Assessed(probe));

        var (result, _) = await Run(Mapping(child).Compile(), NewCase("a", "b"), store: store);

        var failed = Assert.IsType<GraphExecutionResult<Case>.NodeFailed>(result);
        Assert.Equal("Investigate", failed.Node);
        Assert.Equal("item 'b': failed at 'assess' after 1 attempt(s): rate limited", failed.Message);
        var checkpoint = store.Load("t")!;
        Assert.Equal(CheckpointStatus.Failed, checkpoint.Status);
        Assert.Null(checkpoint.Nested);
        Assert.Equal("transient", checkpoint.Metadata!["error_kind"]);
        Assert.Equal("5000", checkpoint.Metadata!["strata.retry_after_ms"]);
    }

    [Fact]
    public async Task A_Failing_Item_Winds_Its_Siblings_Down()
    {
        var waiting = new TaskCompletionSource();
        var child = Child(async (probe, _, ct) =>
        {
            if (probe.Item == "bad")
            {
                await waiting.Task.WaitAsync(ct);
                throw NodeException.Fatal("boom");
            }

            waiting.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return probe;
        });

        var (result, events) = await Run(Mapping(child).Compile(), NewCase("slow", "bad"));

        var failed = Assert.IsType<GraphExecutionResult<Case>.NodeFailed>(result);
        Assert.Equal("item 'bad': failed at 'assess' after 1 attempt(s): boom", failed.Message);
        var story = Story(events);
        var childStopped = IndexOf(story, "node done Investigate/slow/assess Cancelled");
        var itemStopped = IndexOf(story, "item done Investigate/slow Cancelled SiblingFailed");
        Assert.True(childStopped < itemStopped, string.Join('\n', story));
        Assert.True(itemStopped < IndexOf(story, "node done Investigate Failed"));
    }

    [Fact]
    public async Task An_Item_Timeout_Times_Out_Only_That_Item()
    {
        var clock = new FakeTimeProvider();
        var seen = new ConcurrentQueue<BranchOutcomes<Probe>>();
        var child = Child(async (probe, _, ct) =>
        {
            // "ok" ends long before the 45 s item timeout, so a late timer on a slow machine can't time it out too.
            await Task.Delay(probe.Item == "hung" ? TimeSpan.FromHours(1) : TimeSpan.FromSeconds(1), clock, ct);
            return Passed(probe, "assess");
        });
        var options = new MapOptions<Probe> { ItemTimeout = TimeSpan.FromSeconds(45), Partial = PartialResults.KeepSucceeded };

        var (result, events) = await Run(Mapping(child, options, seen).Compile(), NewCase("ok", "hung"), clock);

        Assert.Equal(["ok:assess+summarize"], Completed(result).Found);
        var hung = Assert.Single(seen)["hung"]!;
        Assert.Equal(BranchStatus.TimedOut, hung.Status);
        Assert.Equal("item 'hung' timed out after 45000 ms", hung.Error!.Message);
        Assert.Equal(NodeFailureKind.Transient, hung.Error.Kind);
        var story = Story(events);
        Assert.True(
            IndexOf(story, "node done Investigate/hung/assess Cancelled") < IndexOf(story, "item done Investigate/hung Failed TimedOut"),
            string.Join('\n', story));
    }

    [Fact]
    public async Task FirstSuccess_Winds_The_Losers_Down()
    {
        var child = Child(async (probe, _, ct) =>
        {
            if (probe.Item == "slow")
            {
                await Task.Delay(Timeout.Infinite, ct);
            }

            return Passed(probe, "assess");
        });

        var (result, events) = await Run(
            Mapping(child, new MapOptions<Probe> { Join = JoinPolicy<Probe>.FirstSuccess }).Compile(), NewCase("slow", "fast"));

        Assert.Equal(["fast:assess+summarize"], Completed(result).Found);
        var story = Story(events);
        var decided = IndexOf(story, "decided Investigate first_success met [fast]");
        Assert.True(IndexOf(story, "item done Investigate/fast Succeeded") < decided);
        var childStopped = IndexOf(story, "node done Investigate/slow/assess Cancelled");
        var loser = IndexOf(story, "item done Investigate/slow Cancelled LostRace");
        Assert.True(decided < childStopped && childStopped < loser, string.Join('\n', story));
    }

    [Fact]
    public async Task An_Empty_Race_Fails_Fatally()
    {
        var graph = Mapping(Child(), new MapOptions<Probe> { Join = JoinPolicy<Probe>.FirstSuccess })
            .SetRecoveryPolicy("Investigate", RecoveryPolicy.Retry(3))
            .Compile();

        var (result, events) = await Run(graph, NewCase());

        var failed = Assert.IsType<GraphExecutionResult<Case>.NodeFailed>(result);
        Assert.Equal((1, "no items to run"), (failed.Attempts, failed.Message));
        Assert.Contains("decided Investigate first_success unmet []", Story(events));
    }

    // --- Composition, streaming, retries, cancellation, hops ---------------------------------

    [Fact]
    public async Task A_Map_Inside_A_Subgraph_Names_Paths_Through_Both()
    {
        var inner = Mapping(Child()).Compile();
        var graph = new StateGraph<Case>()
            .AddSubgraph("Outer", inner, c => c, (_, child) => child)
            .Compile();

        var (result, events) = await Run(graph, NewCase("h1"));

        Assert.Equal(["h1:assess+summarize"], Completed(result).Found);
        var story = Story(events);
        IndexOf(story, "expanded Outer/Investigate [h1]");
        IndexOf(story, "item Outer/Investigate/h1");
        IndexOf(story, "node done Outer/Investigate/h1/summarize Committed");
    }

    [Fact]
    public async Task Item_Chunks_Stream_Under_The_Item_Path()
    {
        var child = new StateGraph<Probe>()
            .AddStreamingNode("speak", async (probe, ctx, ct) =>
            {
                await ctx.Stream!.EmitTokenAsync(probe.Item, ct);
                return probe;
            })
            .Compile();
        var graph = Mapping(child, new MapOptions<Probe> { MaxConcurrency = 1 }).Compile();

        var (reader, completion) = graph.RunStreaming("t", NewCase("a", "b"), new InMemoryCheckpointStore());
        var tokens = new List<(string, string)>();
        await foreach (var chunk in reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            if (chunk.Type == StreamChunkType.Token)
            {
                tokens.Add((chunk.NodeName, chunk.Payload.ToString()));
            }
        }

        Assert.IsType<GraphExecutionResult<Case>.Completed>(await completion);
        Assert.Equal([("Investigate/a/speak", "a"), ("Investigate/b/speak", "b")], tokens);

        // A plain run refuses the map before reading any item.
        var (plain, events) = await Run(graph, NewCase("a"));
        Assert.IsAssignableFrom<GraphExecutionResult<Case>.Failed>(plain);
        Assert.DoesNotContain(events, e => e is MapExpanded);
    }

    [Fact]
    public async Task A_Retried_Map_Reruns_Every_Item()
    {
        var runs = new ConcurrentQueue<string>();
        var failedOnce = 0;
        var child = Child((probe, _, _) =>
        {
            runs.Enqueue(probe.Item);
            return probe.Item == "b" && Interlocked.Exchange(ref failedOnce, 1) == 0
                ? ValueTask.FromException<Probe>(NodeException.Transient("flaky"))
                : Assessed(probe);
        });
        var graph = Mapping(child, new MapOptions<Probe> { MaxConcurrency = 1 })
            .SetRecoveryPolicy("Investigate", RecoveryPolicy.Retry(1))
            .Compile();

        var (result, events) = await Run(graph, NewCase("a", "b"));

        Assert.Equal(["a:assess+summarize", "b:assess+summarize"], Completed(result).Found);
        Assert.Equal(["a", "b", "a", "b"], runs);
        Assert.Equal(
            [("Investigate", 1), ("Investigate", 2)],
            events.OfType<MapExpanded>().Select(e => (e.NodePath!, e.Attempt!.Value)));
    }

    [Fact]
    public async Task A_Cancelled_Run_Winds_Items_Down_And_Checkpoints_The_Map()
    {
        using var cts = new CancellationTokenSource();
        var started = 0;
        var child = Child(async (probe, _, ct) =>
        {
            if (Interlocked.Increment(ref started) == 2)
            {
                await cts.CancelAsync();
            }

            await Task.Delay(Timeout.Infinite, ct);
            return probe;
        });
        var store = new InMemoryCheckpointStore();

        var (result, events) = await Run(Mapping(child).Compile(), NewCase("a", "b"), store: store, cancellationToken: cts.Token);

        Assert.Equal("Investigate", Assert.IsType<GraphExecutionResult<Case>.Cancelled>(result).Node);
        var checkpoint = store.Load("t")!;
        Assert.Equal((CheckpointStatus.Cancelled, "Investigate"), (checkpoint.Status, checkpoint.NextNode));
        Assert.Null(checkpoint.Nested);
        var story = Story(events);
        foreach (var item in new[] { "a", "b" })
        {
            Assert.True(
                IndexOf(story, $"node done Investigate/{item}/assess Cancelled") < IndexOf(story, $"item done Investigate/{item} Cancelled"),
                string.Join('\n', story));
        }

        Assert.True(IndexOf(story, "item done Investigate/b Cancelled") < IndexOf(story, "node done Investigate Cancelled"));
    }

    [Fact]
    public async Task A_Map_Is_One_Hop_And_Each_Item_Counts_Its_Own()
    {
        var oneHop = new CompilationOptions { MaxNodeHops = 1 };
        var (result, _) = await Run(Mapping(Child()).Compile(oneHop), NewCase("a", "b", "c"));
        Assert.Equal(3, Completed(result).Found.Count);

        var (tripped, events) = await Run(Mapping(Child(options: oneHop)).Compile(oneHop), NewCase("a"));
        var failed = Assert.IsType<GraphExecutionResult<Case>.NodeFailed>(tripped);
        Assert.Equal(
            "item 'a': hit its hop limit: hop limit of 1 reached: node 'summarize' would be hop 2",
            failed.Message);
        Assert.Contains(events, e => e is HopLimitExceeded && e.NodePath == "Investigate/a/summarize");
    }

    // --- Compile-time checks ---------------------------------------------------------------

    [Fact]
    public void A_Child_With_An_Interrupt_Boundary_Is_Refused_At_Compile_Time()
    {
        var direct = new StateGraph<Probe>().AddNode("assess", p => p).AddInterruptBoundary("assess").Compile();
        var error = Assert.Throws<InvalidOperationException>(() => Mapping(direct).Compile());
        Assert.Equal("map 'Investigate' runs a graph with an interrupt boundary at 'assess'; a map item can't freeze", error.Message);

        var nested = new StateGraph<Probe>()
            .AddNode("assess", p => p)
            .AddSubgraph("Review", direct, p => p, (_, p) => p)
            .AddEdge("assess", "Review")
            .Compile();
        error = Assert.Throws<InvalidOperationException>(() => Mapping(nested).Compile());
        Assert.Equal("map 'Investigate' runs a graph with an interrupt boundary at 'Review/assess'; a map item can't freeze", error.Message);

        // A boundary name that isn't a node never triggers.
        var stray = new StateGraph<Probe>().AddNode("assess", p => p).AddInterruptBoundary("elsewhere").Compile();
        Mapping(stray).Compile();
    }

    [Fact]
    public void An_Invalid_Map_Quorum_Is_Refused_At_Compile_Time()
    {
        var error = Assert.Throws<GraphCompilationException>(
            () => Mapping(Child(), new MapOptions<Probe> { Join = JoinPolicy<Probe>.Quorum(0) }).Compile());

        var invalid = Assert.Single(error.Result.Errors);
        Assert.Equal(("STRATA_ERR_INVALID_QUORUM", "Investigate"), (invalid.Code, invalid.Node));
        Assert.Equal("Map 'Investigate' has a quorum of 0, which isn't at least 1.", invalid.Message);

        // Above the item count is decided at run time, not refused.
        Mapping(Child(), new MapOptions<Probe> { Join = JoinPolicy<Probe>.Quorum(50) }).Compile();
    }

    // --- The definition ----------------------------------------------------------------------

    private static string Fixtures(string folder, [CallerFilePath] string sourceFilePath = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(sourceFilePath)!, "..", "..", "..", "schema", "fixtures", folder));

    /// <summary>The graph <c>map.json</c> describes.</summary>
    private static StateGraph<Case> MapFixture()
    {
        var dig = new StateGraph<Probe>().AddNode("Dig", p => p).Compile();
        var template = new StateGraph<Probe>()
            .AddNode("Assess", p => p)
            .AddParallelGroup("Probe", new ParallelBranches<Probe>().Branch("logs", p => p).Branch("metrics", p => p), (p, _) => p)
            .AddSubgraph("Deep", dig, p => p, (p, _) => p)
            .AddEdge("Assess", "Probe")
            .AddEdge("Probe", "Deep")
            .Compile();
        var check = new StateGraph<Probe>().AddNode("Check", p => p).Compile();

        return new StateGraph<Case>()
            .AddNode("Generate", c => c)
            .AddMap(
                "Investigate", c => c.Items, item => item, template, (_, item) => new Probe(item, []), (c, _) => c,
                metadata: new Dictionary<string, string> { ["key"] = "hypothesis id", ["items"] = "hypotheses" })
            .AddMapWithOutcomes(
                "Confirm", c => c.Items, item => item, check, (_, item) => new Probe(item, []), (c, _) => c,
                new MapOptions<Probe>
                {
                    Join = JoinPolicy<Probe>.FirstMatching(_ => true, "confidence >= 0.9"),
                    Partial = PartialResults.KeepSucceeded,
                    MaxConcurrency = 3,
                    ItemTimeout = TimeSpan.FromSeconds(30),
                    MaxItems = 5,
                })
            .AddParallelGroup(
                "Notify", new ParallelBranches<Case>().Branch("email", c => c).Branch("chat", c => c), (c, _) => c,
                new ParallelGroupOptions<Case> { Join = JoinPolicy<Case>.FirstSuccess })
            .AddEdge("Generate", "Investigate")
            .AddEdge("Investigate", "Confirm")
            .AddEdge("Confirm", "Notify");
    }

    [Fact]
    public void The_Map_Is_Exported_As_The_Shared_Fixture_Has_It()
    {
        var expected = JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures("graph-export"), "map.json")));
        var exported = JsonSerializer.SerializeToNode(MapFixture().Compile().ToDefinition());

        Assert.True(JsonNode.DeepEquals(expected, exported), $"Expected: {expected}\nActual: {exported}");
    }

    [Fact]
    public void The_Map_Fixture_Round_Trips_And_Validates()
    {
        var raw = JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures("graph-export"), "map.json")))!;
        var definition = raw.Deserialize<GraphDefinition>()!;

        Assert.True(JsonNode.DeepEquals(raw, JsonSerializer.SerializeToNode(definition.ToTopology().ToDefinition())));
        Assert.True(GraphValidator.Validate(definition.ToTopology()).IsValid);
    }

    [Fact]
    public void ToTopology_Refuses_A_Misplaced_Map()
    {
        var old = JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures("graph-export"), "map.json")))!;
        old["schema_version"] = "1.2.0";
        Assert.Equal(
            "node 'Investigate' is a map, which schema_version 1.2.0 doesn't have",
            Assert.Throws<FormatException>(() => old.Deserialize<GraphDefinition>()!.ToTopology()).Message);

        var branches = JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures("graph-export"), "map.json")))!;
        branches["nodes"]![1]!["branches"] = new JsonArray();
        Assert.Equal(
            "node 'Investigate' is a map but has both branches and graph",
            Assert.Throws<FormatException>(() => branches.Deserialize<GraphDefinition>()!.ToTopology()).Message);
    }

    [Fact]
    public void Validation_Names_Template_Nodes_With_A_Star()
    {
        var raw = File.ReadAllText(Path.Combine(Fixtures("graph-validation"), "map-template-errors.json"));
        var result = GraphValidator.Validate(JsonSerializer.Deserialize<GraphDefinition>(raw)!.ToTopology());

        Assert.Equal(
            [
                ("STRATA_ERR_INVALID_QUORUM", "Vote", (string?)null),
                ("STRATA_ERR_ORPHAN", "Investigate/*/Stray", null),
                ("STRATA_ERR_UNGUARDED_CYCLE", "Review/Fan/*/Loop", "Review/Fan/*/Read"),
            ],
            result.Errors.Select(e => (e.Code, e.Node, e.RelatedNode)));
    }
}
