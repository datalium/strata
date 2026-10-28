using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Datalium.Strata.Graph;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests.Tools;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/tool_middleware.rs</c> (<c>design/23-Tool-Middleware.md</c>, SG-22):
/// middleware order, short-circuiting, a middleware's own refusal and failure, a timeout that stops
/// waiting, context and argument changes, subsets sharing the chain, refused calls reaching the
/// on-refused hooks, and middleware running inside the <c>strata.tool</c> span. Keep the two in step.
/// </summary>
public class ToolMiddlewareTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(int N);

    private static (ToolRegistry Tools, MiddlewareTools.Probe Probe) Registry()
    {
        var probe = new MiddlewareTools.Probe();
        return (new ToolRegistry().Register(new MiddlewareTools(probe)), probe);
    }

    private static string Result(string json) => JsonDocument.Parse(json).RootElement.GetProperty("result").GetString()!;

    private static string Echo(string text) => JsonSerializer.Serialize(new { text });

    // --- Order ------------------------------------------------------------------

    [Fact]
    public async Task Middleware_Runs_In_Registration_Order_Outermost_First()
    {
        var trace = new List<string>();
        var (tools, _) = Registry();
        tools.Use(new Recording("a", trace)).Use(new Recording("b", trace)).Use(new Recording("c", trace));

        Assert.Equal("hi", Result(await tools.DispatchToolAsync("mw.echo", Echo("hi"))));

        Assert.Equal(["a in", "b in", "c in", "c out", "b out", "a out"], trace);
        Assert.Equal(3, tools.Middleware.Count);
    }

    [Fact]
    public async Task A_Registry_Without_Middleware_Dispatches_As_Before()
    {
        var (tools, probe) = Registry();

        Assert.Empty(tools.Middleware);
        Assert.Equal("hi", Result(await tools.DispatchToolAsync("mw.echo", Echo("hi"))));
        await Assert.ThrowsAsync<ToolNotFoundException>(() => tools.DispatchToolAsync("mw.nope", "{}"));
        Assert.Equal(1, probe.Runs);
    }

    // --- Short-circuit, denial, failure -------------------------------------------

    [Fact]
    public async Task A_Middleware_That_Returns_Without_Calling_Next_Short_Circuits_The_Tool()
    {
        var (tools, probe) = Registry();
        tools.Use(new Cache());

        var (completed, spans) = await InANode("mw-cache", async (ctx, ct) =>
        {
            Assert.Equal("once", Result(await tools.DispatchToolAsync("mw.echo", Echo("once"), ToolCallContext.From(ctx), ct)));
            Assert.Equal("once", Result(await tools.DispatchToolAsync("mw.echo", Echo("once"), ToolCallContext.From(ctx), ct)));
        });

        Assert.Equal(1, probe.Runs);
        Assert.Equal([ToolCallResult.Succeeded, ToolCallResult.Succeeded], completed.Select(c => c.Result));
        var toolSpans = spans.Where(a => a.OperationName == "strata.tool").OrderBy(a => a.StartTimeUtc).ToList();
        Assert.Null(toolSpans[0].GetTagItem("strata.tool.short_circuited"));
        Assert.Equal(true, toolSpans[1].GetTagItem("strata.tool.short_circuited"));
        Assert.Equal("succeeded", toolSpans[1].GetTagItem("strata.tool.result"));
    }

    [Fact]
    public async Task A_Denial_Is_A_Refused_Call_With_Its_Reason()
    {
        var (tools, probe) = Registry();
        tools.Use(new Inline((call, _, _) => throw new ToolCallDeniedException(call.Name, "tenant acme may not echo")));

        var (completed, spans) = await InANode("mw-denied", async (ctx, ct) =>
        {
            var denied = await Assert.ThrowsAsync<ToolCallDeniedException>(
                () => tools.DispatchToolAsync("mw.echo", Echo("x"), ToolCallContext.From(ctx), ct));
            Assert.Equal(("mw.echo", "tenant acme may not echo"), (denied.ToolName, denied.Reason));
        });

        Assert.Equal(0, probe.Runs);
        var call = Assert.Single(completed);
        Assert.Equal(ToolCallResult.Refused, call.Result);
        Assert.Equal("Tool 'mw.echo' was denied: tenant acme may not echo", call.Error);
        var span = spans.Single(a => a.OperationName == "strata.tool");
        Assert.Equal("refused", span.GetTagItem("strata.tool.result"));
        Assert.Equal("denied", span.GetTagItem("error.type"));
        Assert.Equal(ActivityStatusCode.Error, span.Status);
    }

    [Fact]
    public async Task A_Middleware_Failure_Is_A_Failed_Call()
    {
        var (tools, probe) = Registry();
        tools.Use(new Inline((_, _, _) => throw new InvalidOperationException("the policy backend is down")));

        var (completed, spans) = await InANode("mw-failed", async (ctx, ct) =>
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => tools.DispatchToolAsync("mw.echo", Echo("x"), ToolCallContext.From(ctx), ct)));

        Assert.Equal(0, probe.Runs);
        Assert.Equal(ToolCallResult.Failed, Assert.Single(completed).Result);
        Assert.Equal("System.InvalidOperationException", spans.Single(a => a.OperationName == "strata.tool").GetTagItem("error.type"));
    }

    [Fact]
    public async Task A_Timeout_Middleware_Stops_Waiting_For_A_Tool_That_Ignores_Its_Token()
    {
        var (tools, probe) = Registry();
        tools.Use(new TimeoutMiddleware(TimeSpan.FromMilliseconds(100)));

        var (completed, _) = await InANode("mw-timeout", async (ctx, ct) =>
            await Assert.ThrowsAsync<TimeoutException>(
                () => tools.DispatchToolAsync("mw.stall", "{}", ToolCallContext.From(ctx), ct).WaitAsync(Patience)));
        probe.Release.TrySetResult();

        Assert.Equal(1, probe.Runs);
        // The middleware gave up, not the run: a failure, not a cancellation.
        Assert.Equal(ToolCallResult.Failed, Assert.Single(completed).Result);
    }

    // --- What a middleware may change -----------------------------------------------

    [Fact]
    public async Task A_Middleware_Can_Enrich_The_Context_And_Replace_The_Arguments()
    {
        var (tools, probe) = Registry();
        tools.Use(new Inline((call, next, ct) => next(
            call with
            {
                Arguments = Echo("redacted"),
                Context = call.Context with { CallerId = "gateway", Metadata = new Dictionary<string, string> { ["tenant"] = "acme" } },
            },
            ct)));

        var result = await tools.DispatchToolAsync("mw.echo", Echo("secret"), new ToolCallContext { CallId = "c1" });

        Assert.Equal("redacted", Result(result));
        var seen = Assert.Single(probe.Seen);
        Assert.Equal(("gateway", "acme", "c1"), (seen.CallerId, seen.Metadata["tenant"], seen.CallId));
    }

    [Fact]
    public void A_Calls_Name_And_Descriptor_Cannot_Be_Changed_With_With()
    {
        // `with` sets init accessors only; these two have none.
        Assert.Null(typeof(ToolCall).GetProperty(nameof(ToolCall.Name))!.SetMethod);
        Assert.Null(typeof(ToolCall).GetProperty(nameof(ToolCall.Descriptor))!.SetMethod);
        Assert.NotNull(typeof(ToolCall).GetProperty(nameof(ToolCall.Arguments))!.SetMethod);
    }

    [Fact]
    public async Task A_Middleware_Sees_The_Tools_Descriptor_And_Can_Validate_Against_It()
    {
        var (tools, probe) = Registry();
        tools.Use(new RequiredArguments());

        var denied = await Assert.ThrowsAsync<ToolCallDeniedException>(() => tools.DispatchToolAsync("mw.echo", "{}"));

        Assert.Equal("missing required argument 'text'", denied.Reason);
        Assert.Equal("ok", Result(await tools.DispatchToolAsync("mw.echo", Echo("ok"))));
        Assert.Equal(1, probe.Runs);
    }

    [Fact]
    public async Task A_Middleware_May_Call_Next_More_Than_Once()
    {
        var (tools, probe) = Registry();
        tools.Use(new Inline(async (call, next, ct) =>
        {
            await next(call, ct);
            return await next(call, ct);
        }));

        Assert.Equal("twice", Result(await tools.DispatchToolAsync("mw.echo", Echo("twice"))));
        Assert.Equal(2, probe.Runs);
    }

    // --- Subsets and composition ------------------------------------------------------

    [Fact]
    public async Task A_Subset_Shares_Its_Parents_Middleware_And_Adds_Its_Own_Inside_It()
    {
        var trace = new List<string>();
        var (parent, _) = Registry();
        parent.Use(new Recording("gateway", trace));
        var subset = parent.Subset("mw.echo").Use(new Recording("agent", trace));
        var earlier = parent.Subset("mw.echo");
        parent.Use(new Recording("late", trace));

        await subset.DispatchToolAsync("mw.echo", Echo("1"));
        Assert.Equal(["gateway in", "agent in", "agent out", "gateway out"], trace);

        trace.Clear();
        await parent.DispatchToolAsync("mw.echo", Echo("2"));
        Assert.Equal(["gateway in", "late in", "late out", "gateway out"], trace);

        trace.Clear();
        await earlier.DispatchToolAsync("mw.echo", Echo("3"));
        Assert.Equal(["gateway in", "gateway out"], trace);
    }

    [Fact]
    public async Task Registering_A_Registry_Keeps_Its_Middleware_On_Its_Tools()
    {
        var trace = new List<string>();
        var (inner, _) = Registry();
        inner.Use(new Recording("inner", trace));
        var outer = new ToolRegistry().Register(inner).Register(new LegacyProvider()).Use(new Recording("outer", trace));

        await outer.DispatchToolAsync("mw.echo", Echo("1"));
        Assert.Equal(["outer in", "inner in", "inner out", "outer out"], trace);

        trace.Clear();
        await outer.DispatchToolAsync("legacy", "{}");
        Assert.Equal(["outer in", "outer out"], trace);
    }

    // --- Refused calls -----------------------------------------------------------------

    [Fact]
    public async Task Refused_Calls_Reach_Every_On_Refused_Hook_And_No_Invoke()
    {
        var trace = new List<string>();
        var (parent, _) = Registry();
        var tools = parent.Subset("mw.echo").Use(new Recording("a", trace)).Use(new Recording("b", trace));

        await Assert.ThrowsAsync<ToolNotPermittedException>(() => tools.DispatchToolAsync("mw.other", "{}"));
        await Assert.ThrowsAsync<ToolNotFoundException>(() => tools.DispatchToolAsync("mw.nope", "{}"));

        Assert.Equal(
            [
                "a refused mw.other ToolNotPermittedException",
                "b refused mw.other ToolNotPermittedException",
                "a refused mw.nope ToolNotFoundException",
                "b refused mw.nope ToolNotFoundException",
            ],
            trace);
    }

    [Fact]
    public async Task A_Refused_Call_Hook_Sees_No_Descriptor_And_Runs_Even_When_Cancelled()
    {
        ToolCall? seen = null;
        var (tools, _) = Registry();
        tools.Use(new Inline((call, next, ct) => next(call, ct), (call, _) => seen = call));
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAsync<ToolNotFoundException>(() => tools.DispatchToolAsync("mw.nope", "{}", ToolCallContext.None, cancelled.Token));

        Assert.Equal("mw.nope", seen!.Name);
        Assert.Null(seen.Descriptor);
    }

    [Fact]
    public async Task A_Failing_On_Refused_Hook_Replaces_The_Refusal_And_Stops_The_Rest()
    {
        var trace = new List<string>();
        var (tools, _) = Registry();
        tools
            .Use(new Inline((call, next, ct) => next(call, ct), (_, _) => throw new InvalidOperationException("audit is down")))
            .Use(new Recording("after", trace));

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => tools.DispatchToolAsync("mw.nope", "{}"));

        Assert.Equal("audit is down", failure.Message);
        Assert.Empty(trace);
    }

    // --- Telemetry and cancellation ---------------------------------------------------

    [Fact]
    public async Task Middleware_Runs_Inside_The_Tool_Span()
    {
        var (tools, _) = Registry();
        tools.Use(new Inline(async (call, next, ct) =>
        {
            Activity.Current?.SetTag("test.middleware", "was here");
            await Task.Delay(50, ct);
            return await next(call, ct);
        }));

        var (completed, spans) = await InANode("mw-span", async (ctx, ct) =>
            await tools.DispatchToolAsync("mw.echo", Echo("x"), ToolCallContext.From(ctx), ct));

        Assert.True(Assert.Single(completed).Duration >= TimeSpan.FromMilliseconds(45));
        var span = spans.Single(a => a.OperationName == "strata.tool");
        Assert.Equal("was here", span.GetTagItem("test.middleware"));
        Assert.True(span.Duration >= TimeSpan.FromMilliseconds(45));
    }

    [Fact]
    public async Task An_Already_Cancelled_Call_Runs_No_Middleware()
    {
        var trace = new List<string>();
        var (tools, probe) = Registry();
        tools.Use(new Recording("a", trace));
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => tools.DispatchToolAsync("mw.echo", Echo("x"), ToolCallContext.None, cancelled.Token));

        Assert.Empty(trace);
        Assert.Equal(0, probe.Runs);
    }

    [Fact]
    public async Task Cancelling_The_Callers_Token_During_The_Chain_Is_A_Cancellation()
    {
        var (tools, _) = Registry();
        using var cts = new CancellationTokenSource();
        tools.Use(new Inline(async (call, next, ct) =>
        {
            await cts.CancelAsync();
            ct.ThrowIfCancellationRequested();
            return await next(call, ct);
        }));

        var (completed, _) = await InANode("mw-cancelled", async (ctx, _) =>
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => tools.DispatchToolAsync("mw.echo", Echo("x"), ToolCallContext.From(ctx), cts.Token)));

        Assert.Equal(ToolCallResult.Cancelled, Assert.Single(completed).Result);
    }

    // --- Helpers ------------------------------------------------------------------------

    /// <summary>
    /// Runs <paramref name="calls"/> inside a one-node graph and returns the tool calls'
    /// <see cref="ToolCallCompleted"/> events and the run's spans.
    /// </summary>
    private static async Task<(List<ToolCallCompleted> Completed, List<Activity> Spans)> InANode(
        string threadId, Func<NodeContext, CancellationToken, Task> calls)
    {
        using var spans = new Spans();
        var graph = new StateGraph<Job>()
            .AddNode("Agent", async (Job job, NodeContext ctx, CancellationToken ct) =>
            {
                await calls(ctx, ct);
                return job;
            })
            .Compile();
        var channel = new RunEventChannel();

        await graph.RunAsync(threadId, new Job(1), new InMemoryCheckpointStore(), new RunOptions { Observer = channel }).WaitAsync(Patience);
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        return ([.. events.OfType<ToolCallCompleted>()], spans.OfRun(threadId));
    }

    /// <summary>Collects every stopped activity from the engine's source.</summary>
    private sealed class Spans : IDisposable
    {
        private readonly ActivityListener _listener;

        public Spans()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == StrataTelemetry.Name,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = Stopped.Add,
            };
            ActivitySource.AddActivityListener(_listener);
        }

        private ConcurrentBag<Activity> Stopped { get; } = [];

        public List<Activity> OfRun(string threadId)
        {
            var run = Stopped.Single(a => a.OperationName == "strata.run" && (string?)a.GetTagItem("strata.thread_id") == threadId);
            return [.. Stopped.Where(a => a.TraceId == run.TraceId)];
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>Records entering and leaving the chain, and refused calls.</summary>
    private sealed class Recording(string name, List<string> trace) : IToolMiddleware
    {
        public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            Add($"{name} in");
            var result = await next(call, cancellationToken);
            Add($"{name} out");
            return result;
        }

        public Task OnRefusedAsync(ToolCall call, ToolNotFoundException refusal, CancellationToken cancellationToken)
        {
            Add($"{name} refused {call.Name} {refusal.GetType().Name}");
            return Task.CompletedTask;
        }

        private void Add(string entry)
        {
            lock (trace)
            {
                trace.Add(entry);
            }
        }
    }

    /// <summary>A middleware made of lambdas.</summary>
    private sealed class Inline(
        Func<ToolCall, ToolCallDelegate, CancellationToken, Task<string>> invoke,
        Action<ToolCall, ToolNotFoundException>? onRefused = null) : IToolMiddleware
    {
        public Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken) =>
            invoke(call, next, cancellationToken);

        public Task OnRefusedAsync(ToolCall call, ToolNotFoundException refusal, CancellationToken cancellationToken)
        {
            onRefused?.Invoke(call, refusal);
            return Task.CompletedTask;
        }
    }

    /// <summary>Answers a repeated (tool, arguments) pair from memory, as StrataOps' read cache does.</summary>
    private sealed class Cache : IToolMiddleware
    {
        private readonly ConcurrentDictionary<(string, string), string> _results = new();

        public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            if (_results.TryGetValue((call.Name, call.Arguments), out var cached))
            {
                return cached;
            }

            var result = await next(call, cancellationToken);
            _results[(call.Name, call.Arguments)] = result;
            return result;
        }
    }

    /// <summary>Stops waiting after <paramref name="limit"/>, even for a tool that ignores its token.</summary>
    private sealed class TimeoutMiddleware(TimeSpan limit) : IToolMiddleware
    {
        public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(limit);
            return await next(call, timeout.Token).WaitAsync(limit, cancellationToken);
        }
    }

    /// <summary>
    /// A sample, not shipped (<c>design/23-Tool-Middleware.md</c>, "Not built"): denies a call missing
    /// an argument its manifest entry requires.
    /// </summary>
    private sealed class RequiredArguments : IToolMiddleware
    {
        public Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            if (call.Descriptor is { } descriptor && descriptor.Parameters.TryGetProperty("required", out var required))
            {
                using var arguments = JsonDocument.Parse(call.Arguments);
                foreach (var name in required.EnumerateArray().Select(r => r.GetString()!))
                {
                    if (!arguments.RootElement.TryGetProperty(name, out _))
                    {
                        throw new ToolCallDeniedException(call.Name, $"missing required argument '{name}'");
                    }
                }
            }

            return next(call, cancellationToken);
        }
    }
}

/// <summary>Tools for <see cref="ToolMiddlewareTests"/>, named so no other suite adds to their metric series.</summary>
public partial class MiddlewareTools(MiddlewareTools.Probe probe)
{
    /// <summary>How often the tools ran, what context they saw, and a gate that releases <c>mw.stall</c>.</summary>
    public sealed class Probe
    {
        private int _runs;

        public int Runs => Volatile.Read(ref _runs);

        public ConcurrentQueue<ToolCallContext> Seen { get; } = new();

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Ran() => Interlocked.Increment(ref _runs);
    }

    [StrataTool(Name = "mw.echo", Description = "Echoes its text.")]
    public string Echo(string text, ToolCallContext context)
    {
        probe.Ran();
        probe.Seen.Enqueue(context);
        return text;
    }

    [StrataTool(Name = "mw.stall", Description = "Ignores its token.")]
    public async Task<string> StallAsync()
    {
        probe.Ran();
        await probe.Release.Task;
        return "late";
    }

    [StrataTool(Name = "mw.other", Description = "Another tool.")]
    public string Other()
    {
        probe.Ran();
        return "other";
    }
}
