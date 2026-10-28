using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/telemetry.rs</c> (<c>design/18-Run-Observability.md</c>, SG-19):
/// the span tree the engine builds (run → node → attempt, branches under their group's attempt, a
/// subgraph's nodes under its attempt), a handler's own span parented under its attempt or branch, and
/// what each metric records. Listeners are process-wide, so every test filters by a trace or by node
/// names no other test uses.
/// </summary>
public class TelemetryTests
{
    private static readonly ActivitySource HandlerSource = new("Datalium.Strata.Tests.Handlers");

    public sealed record Job(int N);

    /// <summary>Collects every stopped activity from the engine's source and the test handlers' source.</summary>
    private sealed class Spans : IDisposable
    {
        private readonly ActivityListener _listener;

        public Spans()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name is StrataTelemetry.Name or "Datalium.Strata.Tests.Handlers",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => Stopped.Add(activity),
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public ConcurrentBag<Activity> Stopped { get; } = [];

        /// <summary>The spans of the run whose run span has <paramref name="threadId"/>.</summary>
        public List<Activity> OfRun(string threadId)
        {
            var run = Stopped.Single(a => a.OperationName == "strata.run" && (string?)a.GetTagItem("strata.thread_id") == threadId);
            return [.. Stopped.Where(a => a.TraceId == run.TraceId)];
        }

        public void Dispose() => _listener.Dispose();
    }

    private static Activity Named(IEnumerable<Activity> spans, string displayName) =>
        spans.Single(a => a.DisplayName == displayName);

    private static ValueTask<Job> HandlerSpan(Job job, string name)
    {
        using var own = HandlerSource.StartActivity(name);
        return ValueTask.FromResult(job);
    }

    [Fact]
    public async Task The_Engine_Builds_The_Documented_Span_Tree()
    {
        using var spans = new Spans();
        var calls = 0;
        var child = new StateGraph<Job>()
            .AddNode("Draft", (job, _, _) => HandlerSpan(job, "draft-call"))
            .Compile();
        var graph = new StateGraph<Job>()
            .AddFallibleNode("Classify", (job, _, _) => Interlocked.Increment(ref calls) == 1
                ? throw NodeException.Transient("gateway timeout")
                : HandlerSpan(job, "classify-call"))
            .AddSubgraph("Work", child, job => job, (_, job) => job)
            .AddParallelGroup(
                "Gather",
                new ParallelBranches<Job>()
                    .Branch("Acme", (job, _, _) => HandlerSpan(job, "acme-call"))
                    .Branch("Globex", job =>
                    {
                        using var own = HandlerSource.StartActivity("globex-call");
                        return job;
                    }),
                (state, _) => state)
            .AddNode("Sync", job =>
            {
                using var own = HandlerSource.StartActivity("sync-call");
                return job;
            })
            .AddEdge("Classify", "Work")
            .AddEdge("Work", "Gather")
            .AddEdge("Gather", "Sync")
            .SetRecoveryPolicy("Classify", RecoveryPolicy.Retry(1))
            .Compile();

        await graph.RunAsync(
            "span-tree", new Job(1), new InMemoryCheckpointStore(),
            new RunOptions { Metadata = { ["tenant"] = "acme" }, RunId = "exec-1" });
        var run = spans.OfRun("span-tree");

        var root = Named(run, "strata.run");
        Assert.Equal("acme", root.GetTagItem("strata.metadata.tenant"));
        Assert.Equal("exec-1", root.GetTagItem("strata.run_id"));
        Assert.Equal("completed", root.GetTagItem("strata.outcome"));

        var classify = Named(run, "Classify");
        Assert.Equal(root.SpanId, classify.ParentSpanId);
        Assert.Equal(1, classify.GetTagItem("strata.retry_count"));
        var first = Named(run, "Classify #1");
        Assert.Equal(classify.SpanId, first.ParentSpanId);
        Assert.Equal(ActivityStatusCode.Error, first.Status);
        Assert.Equal("transient", first.GetTagItem("error.type"));
        Assert.Contains(classify.Events, e => e.Name == "retry");
        var second = Named(run, "Classify #2");
        Assert.Equal(classify.SpanId, second.ParentSpanId);
        Assert.Equal(second.SpanId, Named(run, "classify-call").ParentSpanId);

        // A subgraph's nodes nest under the subgraph node's attempt.
        var workAttempt = Named(run, "Work #1");
        var draft = Named(run, "Work/Draft");
        Assert.Equal(workAttempt.SpanId, draft.ParentSpanId);
        Assert.Equal(Named(run, "Work/Draft #1").SpanId, Named(run, "draft-call").ParentSpanId);

        // Branches nest under their group's attempt, and their handlers (async and sync) under them.
        var gatherAttempt = Named(run, "Gather #1");
        var acme = Named(run, "Gather/Acme");
        var globex = Named(run, "Gather/Globex");
        Assert.Equal(gatherAttempt.SpanId, acme.ParentSpanId);
        Assert.Equal(gatherAttempt.SpanId, globex.ParentSpanId);
        Assert.Equal(acme.SpanId, Named(run, "acme-call").ParentSpanId);
        Assert.Equal(globex.SpanId, Named(run, "globex-call").ParentSpanId);

        // A sync handler on the thread pool still runs inside its attempt.
        Assert.Equal(Named(run, "Sync #1").SpanId, Named(run, "sync-call").ParentSpanId);
    }

    // --- Tool calls (design/20-Tool-Dispatch-And-Registry.md) ------------------

    private static ToolRegistry TelemetryRegistry() =>
        new ToolRegistry().Register(new TelemetryTools()).Subset("telemetry.lookup");

    /// <summary>A node that calls <paramref name="tool"/> as <c>investigator</c>, with call id <c>call-&lt;tool&gt;</c>.</summary>
    private static Func<Job, NodeContext, CancellationToken, ValueTask<Job>> CallsTool(ToolRegistry tools, string tool) =>
        async (job, ctx, ct) =>
        {
            try
            {
                await tools.DispatchToolAsync(
                    tool, "{}", ToolCallContext.From(ctx) with { CallerId = "investigator", CallId = $"call-{tool}" }, ct);
            }
            catch (ToolNotFoundException)
            {
                // A refused call is what some of these tests look at; its span records it.
            }

            return job;
        };

    [Fact]
    public async Task A_Tool_Call_Is_A_Span_Under_The_Calling_Attempt_And_The_Tool_Runs_Inside_It()
    {
        using var spans = new Spans();
        var graph = new StateGraph<Job>()
            .AddNode("ToolCaller", CallsTool(TelemetryRegistry(), "telemetry.lookup"))
            .Compile();

        await graph.RunAsync("tool-spans", new Job(1), new InMemoryCheckpointStore());

        var run = spans.OfRun("tool-spans");
        var tool = Named(run, "execute_tool telemetry.lookup");
        Assert.Equal("strata.tool", tool.OperationName);
        Assert.Equal(ActivityKind.Internal, tool.Kind);
        Assert.Equal(Named(run, "ToolCaller #1").SpanId, tool.ParentSpanId);
        Assert.Equal("execute_tool", tool.GetTagItem("gen_ai.operation.name"));
        Assert.Equal("telemetry.lookup", tool.GetTagItem("gen_ai.tool.name"));
        Assert.Equal("call-telemetry.lookup", tool.GetTagItem("gen_ai.tool.call.id"));
        Assert.Equal("tool-spans", tool.GetTagItem("strata.thread_id"));
        Assert.Equal("ToolCaller", tool.GetTagItem("strata.node.path"));
        Assert.Equal(1, tool.GetTagItem("strata.attempt"));
        Assert.Equal("investigator", tool.GetTagItem("strata.tool.caller"));
        Assert.Equal("succeeded", tool.GetTagItem("strata.tool.result"));
        Assert.Equal(ActivityStatusCode.Unset, tool.Status);
        Assert.Equal(tool.SpanId, Named(run, "lookup-client-call").ParentSpanId);
    }

    [Fact]
    public async Task A_Span_The_Handler_Opens_Around_A_Tool_Call_Is_Its_Parent()
    {
        using var spans = new Spans();
        var tools = TelemetryRegistry();
        var graph = new StateGraph<Job>()
            .AddNode("ToolIteration", async (Job job, NodeContext ctx, CancellationToken ct) =>
            {
                using var iteration = HandlerSource.StartActivity("agent-iteration");
                await tools.DispatchToolAsync("telemetry.lookup", "{}", ToolCallContext.From(ctx), ct);
                return job;
            })
            .Compile();

        await graph.RunAsync("tool-iteration", new Job(1), new InMemoryCheckpointStore());

        var run = spans.OfRun("tool-iteration");
        var iteration = Named(run, "agent-iteration");
        Assert.Equal(iteration.SpanId, Named(run, "execute_tool telemetry.lookup").ParentSpanId);
        Assert.Equal(Named(run, "ToolIteration #1").SpanId, iteration.ParentSpanId);
    }

    [Fact]
    public async Task A_Tool_Call_Made_Off_The_Nodes_Flow_Still_Parents_To_Its_Attempt()
    {
        using var spans = new Spans();
        var tools = TelemetryRegistry();
        var graph = new StateGraph<Job>()
            .AddNode("ToolSpawner", async (Job job, NodeContext ctx, CancellationToken ct) =>
            {
                // A flow that doesn't carry Activity.Current; the context does.
                Task<string> call;
                using (ExecutionContext.SuppressFlow())
                {
                    call = Task.Run(() => tools.DispatchToolAsync("telemetry.lookup", "{}", ToolCallContext.From(ctx), ct));
                }

                await call;
                return job;
            })
            .Compile();

        await graph.RunAsync("tool-spawned", new Job(1), new InMemoryCheckpointStore());

        var run = spans.OfRun("tool-spawned");
        Assert.Equal(Named(run, "ToolSpawner #1").SpanId, Named(run, "execute_tool telemetry.lookup").ParentSpanId);
    }

    [Fact]
    public async Task A_Refused_Tool_Call_Is_An_Error_Span_With_Its_Reason()
    {
        using var spans = new Spans();
        var tools = TelemetryRegistry();
        var graph = new StateGraph<Job>()
            .AddNode("ToolRefused", CallsTool(tools, "telemetry.other"))
            .AddNode("ToolUnknown", CallsTool(tools, "telemetry.nope"))
            .AddEdge("ToolRefused", "ToolUnknown")
            .Compile();

        await graph.RunAsync("tool-refused", new Job(1), new InMemoryCheckpointStore());

        var run = spans.OfRun("tool-refused");
        foreach (var (display, errorType) in new[]
        {
            ("execute_tool telemetry.other", "not_permitted"),
            ("execute_tool telemetry.nope", "not_found"),
        })
        {
            var tool = Named(run, display);
            Assert.Equal("refused", tool.GetTagItem("strata.tool.result"));
            Assert.Equal(errorType, tool.GetTagItem("error.type"));
            Assert.Equal(ActivityStatusCode.Error, tool.Status);
        }
    }

    [Fact]
    public async Task Arguments_A_Tool_Cant_Take_Are_Invalid_Arguments_And_A_Tools_Own_Exception_Is_Its_Type()
    {
        using var spans = new Spans();
        var tools = new ToolRegistry().Register(new Tools.StrictTools());
        var graph = new StateGraph<Job>()
            .AddNode("Refund", async (Job job, NodeContext ctx, CancellationToken ct) =>
            {
                foreach (var arguments in new[] { "{}", """{"amount":-5}""" })
                {
                    try
                    {
                        await tools.DispatchToolAsync("refund", arguments, ToolCallContext.From(ctx), ct);
                    }
                    catch (ArgumentException)
                    {
                        // Both calls fail; their spans say how.
                    }
                }

                return job;
            })
            .Compile();

        await graph.RunAsync("tool-arguments", new Job(1), new InMemoryCheckpointStore());

        var refunds = spans.OfRun("tool-arguments").Where(a => a.DisplayName == "execute_tool refund")
            .OrderBy(a => a.StartTimeUtc).ToList();
        Assert.Equal(["failed", "failed"], refunds.Select(a => a.GetTagItem("strata.tool.result")));
        Assert.Equal(
            ["invalid_arguments", typeof(ArgumentOutOfRangeException).FullName],
            refunds.Select(a => a.GetTagItem("error.type")));
    }

    [Fact]
    public async Task A_Tool_Call_Outside_A_Run_Parents_To_The_Current_Span()
    {
        using var spans = new Spans();
        var tools = TelemetryRegistry();

        using (var outer = HandlerSource.StartActivity("outside-caller"))
        {
            await tools.DispatchToolAsync("telemetry.lookup", "{}");
        }

        var caller = spans.Stopped.Last(a => a.OperationName == "outside-caller");
        Assert.Contains(spans.Stopped, a => a.OperationName == "strata.tool" && a.ParentSpanId == caller.SpanId);
    }

    [Fact]
    public async Task Tool_Metrics_Carry_Bounded_Labels_Only()
    {
        using var metrics = new Measurements();
        var tools = new ToolRegistry().Register(new TelemetryTools()).Subset("telemetry.metered");

        await tools.DispatchToolAsync("telemetry.metered", "{}");
        await Assert.ThrowsAsync<ToolNotPermittedException>(() => tools.DispatchToolAsync("telemetry.metered_hidden", "{}"));
        await Assert.ThrowsAsync<ToolNotFoundException>(() => tools.DispatchToolAsync("made-up-by-the-model", "{}"));

        var metered = Assert.Single(metrics.Of("strata.tool.requests", "gen_ai.tool.name", "telemetry.metered"));
        Assert.Equal("succeeded", metered.Tags["strata.tool.result"]);
        var hidden = Assert.Single(metrics.Of("strata.tool.requests", "gen_ai.tool.name", "telemetry.metered_hidden"));
        Assert.Equal("refused", hidden.Tags["strata.tool.result"]);
        Assert.Empty(metrics.Of("strata.tool.requests", "gen_ai.tool.name", "made-up-by-the-model"));
        Assert.Contains(metrics.All, m => m.Instrument == "strata.tool.requests"
            && !m.Tags.ContainsKey("gen_ai.tool.name") && Equals(m.Tags["strata.tool.result"], "refused"));
        Assert.Single(metrics.Of("strata.tool.duration", "gen_ai.tool.name", "telemetry.metered"));
    }

    [Fact]
    public async Task The_Context_Exposes_The_Attempt_Span()
    {
        using var spans = new Spans();
        Activity? seen = null;
        Activity? current = null;
        var graph = new StateGraph<Job>()
            .AddNode("Look", (Job job, NodeContext ctx, CancellationToken _) =>
            {
                seen = ctx.Activity;
                current = Activity.Current;
                return ValueTask.FromResult(job);
            })
            .Compile();

        await graph.RunAsync("context-span", new Job(1), new InMemoryCheckpointStore());

        Assert.NotNull(seen);
        Assert.Same(seen, current);
        Assert.Equal("Look #1", seen!.DisplayName);
    }

    [Fact]
    public async Task A_Failed_Run_Marks_Its_Spans_And_A_Cancellation_Does_Not()
    {
        using var spans = new Spans();
        var failing = new StateGraph<Job>()
            .AddFallibleNode("Charge", (Func<Job, Job>)(_ => throw NodeException.Fatal("card declined")))
            .Compile();
        await failing.RunAsync("failed-spans", new Job(1), new InMemoryCheckpointStore());

        var run = spans.OfRun("failed-spans");
        Assert.Equal(ActivityStatusCode.Error, Named(run, "strata.run").Status);
        Assert.Equal("failed", Named(run, "strata.run").GetTagItem("strata.outcome"));
        Assert.Equal("fatal", Named(run, "Charge").GetTagItem("error.type"));
        Assert.Contains(Named(run, "strata.run").Events, e => e.Name == "checkpoint.saved");

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        await failing.RunAsync("cancelled-spans", new Job(1), new InMemoryCheckpointStore(), cts.Token);

        var cancelled = Named(spans.OfRun("cancelled-spans"), "strata.run");
        Assert.Equal(ActivityStatusCode.Unset, cancelled.Status);
        Assert.Equal("requested", cancelled.GetTagItem("strata.cancel_reason"));
    }

    /// <summary>Collects every measurement the engine's instruments record.</summary>
    private sealed class Measurements : IDisposable
    {
        private readonly MeterListener _listener = new();

        public Measurements()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == StrataTelemetry.Name)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
            _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
            _listener.Start();
        }

        public ConcurrentBag<(string Instrument, double Value, Dictionary<string, object?> Tags)> All { get; } = [];

        public List<(double Value, Dictionary<string, object?> Tags)> Of(string instrument, string pathTag, string path) =>
            [.. All.Where(m => m.Instrument == instrument && Equals(m.Tags.GetValueOrDefault(pathTag), path))
                .Select(m => (m.Value, m.Tags))];

        public void Dispose() => _listener.Dispose();

        private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var map = new Dictionary<string, object?>();
            foreach (var tag in tags)
            {
                map[tag.Key] = tag.Value;
            }

            All.Add((instrument.Name, value, map));
        }
    }

    [Fact]
    public async Task Each_Instrument_Records_What_The_Design_Says()
    {
        using var metrics = new Measurements();
        var calls = 0;
        var graph = new StateGraph<Job>()
            .AddFallibleNode("MetricsClassify", job => Interlocked.Increment(ref calls) == 1
                ? throw NodeException.Transient("gateway timeout")
                : job)
            .AddNode("MetricsApprove", job => job)
            .AddInterruptBoundary("MetricsApprove")
            .AddNode("MetricsExecute", job => job)
            .AddEdge("MetricsClassify", "MetricsApprove")
            .AddEdge("MetricsApprove", "MetricsExecute")
            .SetRecoveryPolicy("MetricsClassify", RecoveryPolicy.Retry(1))
            .Compile();
        var store = new InMemoryCheckpointStore();

        await graph.RunAsync("metrics", new Job(1), store);
        await graph.ResumeAsync("metrics", new HumanSignal(true, "x"), store);

        var transitions = metrics.Of("strata.graph.transition", "strata.node.path", "MetricsClassify");
        Assert.Equal("MetricsApprove", Assert.Single(transitions).Tags["strata.next_node.path"]);

        var retries = metrics.Of("strata.node.retries", "strata.node.path", "MetricsClassify");
        Assert.Equal("transient", Assert.Single(retries).Tags["error.type"]);

        foreach (var node in new[] { "MetricsClassify", "MetricsApprove", "MetricsExecute" })
        {
            var duration = Assert.Single(metrics.Of("strata.node.duration", "strata.node.path", node));
            Assert.Equal("committed", duration.Tags["strata.node.result"]);
            Assert.True(duration.Value >= 0);
        }

        // Saves (the freeze and the resume's stamp) and the resume's load.
        var checkpoints = metrics.All.Where(m => m.Instrument == "strata.checkpoint.latency").ToList();
        Assert.Contains(checkpoints, m => Equals(m.Tags["strata.checkpoint.operation"], "save")
            && Equals(m.Tags["strata.checkpoint.status"], "interrupted"));
        Assert.Contains(checkpoints, m => Equals(m.Tags["strata.checkpoint.operation"], "load"));

        Assert.Contains(metrics.All, m => m.Instrument == "strata.graphs.started" && Equals(m.Tags["strata.run.mode"], "resume"));
        Assert.Contains(metrics.All, m => m.Instrument == "strata.graph.duration"
            && Equals(m.Tags["strata.outcome"], "interrupted") && Equals(m.Tags["strata.run.mode"], "run"));
        Assert.Contains(metrics.All, m => m.Instrument == "strata.graphs.active" && m.Value == 1);
        Assert.Contains(metrics.All, m => m.Instrument == "strata.graphs.active" && m.Value == -1);
    }

    private sealed class AlwaysThrows : IRunObserver
    {
        private int _calls;

        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Calls => Volatile.Read(ref _calls);

        public void OnEvent(RunEvent runEvent)
        {
            Interlocked.Increment(ref _calls);
            if (runEvent is RunCompleted)
            {
                Done.TrySetResult();
            }

            throw new InvalidOperationException("observer bug");
        }
    }

    [Fact]
    public async Task Observer_Errors_Are_Counted()
    {
        using var metrics = new Measurements();
        var observer = new AlwaysThrows();
        var graph = new StateGraph<Job>().AddNode("ObserverErrorsOnly", job => job).Compile();

        await graph.RunAsync("observer-errors", new Job(1), new InMemoryCheckpointStore(), new RunOptions { Observer = observer });
        await observer.Done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The last error is counted just after the observer threw. Other tests' observers may throw
        // too, so this waits for at least this run's.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (metrics.All.Count(m => m.Instrument == "strata.observer.errors") < observer.Calls && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.True(metrics.All.Count(m => m.Instrument == "strata.observer.errors") >= observer.Calls);
        Assert.Equal(4, observer.Calls);
    }
}

/// <summary>Tools for <see cref="TelemetryTests"/>, named so no other suite adds to their metric series.</summary>
public partial class TelemetryTools
{
    private static readonly ActivitySource HandlerSource = new("Datalium.Strata.Tests.Handlers");

    /// <summary>Starts an activity of its own, as an HTTP client would.</summary>
    [StrataTool(Name = "telemetry.lookup", Description = "Looks.")]
    public static string Lookup()
    {
        using var client = HandlerSource.StartActivity("lookup-client-call");
        return "found";
    }

    [StrataTool(Name = "telemetry.other", Description = "Other.")]
    public static string Other() => string.Empty;

    [StrataTool(Name = "telemetry.metered", Description = "Metered.")]
    public static string Metered() => string.Empty;

    [StrataTool(Name = "telemetry.metered_hidden", Description = "Hidden.")]
    public static string MeteredHidden() => string.Empty;
}
