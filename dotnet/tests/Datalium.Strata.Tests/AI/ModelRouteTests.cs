using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// Model routes (<c>design/46-Model-Routes.md</c>) beyond the shared cases (<see cref="ModelRouteCaseTests"/>): each
/// attempt a model call in the run, one <see cref="ModelRouted"/>, the <c>route &lt;name&gt;</c> span holding the
/// attempts', the metrics, cancellation, an agent and a model node over a route, a streamed run, the hint reaching the
/// retry, and a route outside a node. Mirrors <c>rust/strata-ai/tests/route.rs</c>.
/// </summary>
public sealed class ModelRouteTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(string Prompt, string Answer = "");

    private static ModelRoute Route(string name, params ModelCandidate[] candidates) =>
        new(new ModelRouteOptions { Name = name, Candidates = candidates });

    private static ModelCandidate Candidate(string name, IChatClient client, string? model = null) =>
        new(name, client, model) { HealthKey = name };

    private static CompiledGraph<Job> OneCall(IChatClient chat, RecoveryPolicy? policy = null)
    {
        var graph = new StateGraph<Job>()
            .AddModelNode<Job>(
                "Ask",
                chat,
                (job, _) => [new(ChatRole.User, job.Prompt)],
                (job, response) => job with { Answer = response.Text },
                new ModelNodeOptions { Chat = new ChatOptions { ModelId = "requested" } });
        if (policy is not null)
        {
            graph.SetRecoveryPolicy("Ask", policy);
        }

        return graph.Compile();
    }

    private static async Task<List<RunEvent>> Drain(RunEventChannel channel)
    {
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        return events;
    }

    private static string Describe(RunEvent e) => e switch
    {
        ModelCallStarted s => $"ModelCallStarted {e.NodePath}#{e.Attempt} {s.Model} {s.Route}",
        ModelCallCompleted c => $"ModelCallCompleted {e.NodePath}#{e.Attempt} {c.Model} {c.Result} {c.Route}",
        ModelRouted r => $"ModelRouted {e.NodePath}#{e.Attempt} {r.Route} {r.Answered} {r.Model} hedges={r.Hedges} "
            + string.Join(",", r.Attempts.Select(a => $"{a.Candidate}:{a.Outcome}:{a.Reason}")),
        _ => e.GetType().Name,
    };

    private static ActivityListener Spans(ConcurrentBag<Activity> spans)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    [Fact]
    public async Task Each_Attempt_Is_A_Model_Call_Of_The_Run_And_The_Route_Is_Recorded_Once_Around_Them()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = Spans(spans);
        var counted = new ConcurrentBag<(string Instrument, long Value, Dictionary<string, object?> Tags)>();
        using var meters = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == StrataTelemetry.Name && instrument.Name.StartsWith("strata.route.", StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        meters.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var map = new Dictionary<string, object?>();
            foreach (var tag in tags)
            {
                map[tag.Key] = tag.Value;
            }

            counted.Add((instrument.Name, value, map));
        });
        meters.Start();
        var frontier = new ScriptedChatClient(Scripted.Fails(new ModelCallException("busy", isTransient: true, statusCode: 503)));
        var local = new ScriptedChatClient(Scripted.Text("a bad deploy"));
        var route = Route("rca-events", Candidate("frontier", frontier, "m-frontier"), Candidate("local", local, "m-local"));
        var channel = new RunEventChannel();

        var result = await OneCall(route).RunAsync("routed", new Job("why?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.Equal("a bad deploy", Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Answer);
        Assert.Equal("m-local", Assert.Single(local.Requests).Options?.ModelId);
        var events = await Drain(channel);
        Assert.Equal(
            [
                "RunStarted", "NodeStarted",
                "ModelCallStarted Ask#1 m-frontier rca-events",
                "ModelCallCompleted Ask#1 m-frontier Failed rca-events",
                "ModelCallStarted Ask#1 m-local rca-events",
                "ModelCallCompleted Ask#1 m-local Succeeded rca-events",
                "ModelRouted Ask#1 rca-events local m-local hedges=0 frontier:Failed:503,local:Answered:",
                "NodeCompleted", "RunCompleted",
            ],
            events.Select(Describe));
        var routed = events.OfType<ModelRouted>().Single();
        Assert.Equal(events.OfType<ModelCallStarted>().Select(s => s.CallId), routed.Attempts.Select(a => a.CallId));

        var routeSpan = spans.Single(a => a.OperationName == "strata.route" && (string?)a.GetTagItem("strata.thread_id") == "routed");
        Assert.Equal("route rca-events", routeSpan.DisplayName);
        Assert.Equal(routed.CallId, routeSpan.GetTagItem("strata.route.call_id"));
        Assert.Equal("local", routeSpan.GetTagItem("strata.route.answered"));
        Assert.Equal(2, routeSpan.GetTagItem("strata.route.attempts"));
        var attemptSpan = spans.Single(a => a.TraceId == routeSpan.TraceId && a.OperationName == "strata.attempt");
        Assert.Equal(attemptSpan.SpanId, routeSpan.ParentSpanId);
        var calls = spans.Where(a => a.TraceId == routeSpan.TraceId && a.OperationName == "strata.model").ToList();
        Assert.Equal(2, calls.Count);
        Assert.All(calls, call =>
        {
            Assert.Equal(routeSpan.SpanId, call.ParentSpanId);
            Assert.Equal("rca-events", call.GetTagItem("strata.route"));
        });
        Assert.Equal(["frontier", "local"], calls.OrderBy(c => c.StartTimeUtc).Select(c => (string?)c.GetTagItem("strata.route.candidate")));

        var mine = counted.Where(m => Equals(m.Tags.GetValueOrDefault("strata.route"), "rca-events")).ToList();
        Assert.Contains(mine, m => m.Instrument == "strata.route.calls" && Equals(m.Tags["strata.outcome"], "answered"));
        Assert.Contains(mine, m => m.Instrument == "strata.route.failovers" && Equals(m.Tags["strata.route.reason"], "failed"));
    }

    /// <summary>
    /// The node cancels its own call (as <c>ModelCallTests</c> does), so every event comes before <c>RunCompleted</c>: a
    /// .NET node the engine gives up on is abandoned, and what it records afterwards is dropped.
    /// </summary>
    [Fact]
    public async Task Cancelling_Mid_Route_Cancels_The_Attempts_In_Flight_And_Leaves_Health_Alone()
    {
        var call = new CancellationTokenSource();
        var slow = new ScriptedChatClient(new Scripted([]) { Hang = true });
        // The hedge's client cancels the call as soon as it is asked, with both attempts in flight.
        var hedge = new Asked(new ScriptedChatClient(new Scripted([]) { Hang = true }), call.Cancel);
        var never = new ScriptedChatClient(Scripted.Text("never"));
        var health = new ModelHealth(new ModelHealthOptions { Breaker = new ModelBreaker(1, TimeSpan.FromMinutes(1)) });
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "cancelled",
            Candidates = [Candidate("slow", slow), Candidate("hedge", hedge), Candidate("never", never)],
            HedgeAfter = TimeSpan.FromMilliseconds(20),
            Health = health,
        });
        var channel = new RunEventChannel();
        var graph = new StateGraph<Job>()
            .AddNode("Ask", async (job, ctx, ct) =>
            {
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, call.Token);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => StrataChat.GetResponseAsync(route, [new(ChatRole.User, job.Prompt)], null, ctx, null, linked.Token));
                return job with { Answer = "gave up" };
            })
            .Compile();

        var result = await graph.RunAsync("cancel-route", new Job("?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        var events = await Drain(channel);
        Assert.Equal(
            [ModelCallResult.Cancelled, ModelCallResult.Cancelled],
            events.OfType<ModelCallCompleted>().Select(c => c.Result));
        var routed = events.OfType<ModelRouted>().Single();
        Assert.Equal(
            [ModelRouteOutcome.Cancelled, ModelRouteOutcome.Cancelled, ModelRouteOutcome.NotTried],
            routed.Attempts.Select(a => a.Outcome));
        Assert.Null(routed.Answered);
        Assert.Equal(1, routed.Hedges);
        Assert.Empty(never.Requests);
        Assert.All(health.Snapshot(), e =>
        {
            Assert.Equal(ModelHealthState.Available, e.State);
            Assert.Equal(0, e.Failures);
        });
    }

    [Fact]
    public async Task An_Agent_Over_A_Route_Fails_Over_On_Its_Second_Turn_With_The_Conversation_So_Far()
    {
        var frontier = new ScriptedChatClient(
            Scripted.Tools(("call-1", "k8s_pods", """{"ns":"shop"}""")),
            Scripted.Fails(new ModelCallException("overloaded", isTransient: true, statusCode: 529)));
        var local = new ScriptedChatClient(Scripted.Text("the checkout pods are crash-looping"));
        var route = Route("rca-agent", Candidate("frontier", frontier), Candidate("local", local));
        var agent = new StrataAgent<string>(route, new ToolRegistry().Register(new AgentTools()), new StrataAgentOptions<string> { Id = "rca-analyst" });
        AgentResult<string>? produced = null;
        var graph = new StateGraph<Job>()
            .AddAgentNode<Job, string>(
                "Investigate",
                agent,
                (job, _) => [new(ChatRole.User, job.Prompt)],
                (job, result) =>
                {
                    produced = result;
                    return job with { Answer = result.Output };
                })
            .Compile();
        var channel = new RunEventChannel();

        var result = await graph.RunAsync("agent-route", new Job("why?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.Equal("the checkout pods are crash-looping", Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Answer);
        Assert.Equal(2, produced!.Turns);
        Assert.Equal("k8s.pods", Assert.Single(produced.ToolCalls).Name);
        // The second turn, failed over, carries the tool's result to the model that answered it.
        var asked = Assert.Single(local.Requests);
        Assert.Contains(asked.Messages, m => m.Role == ChatRole.Tool);
        Assert.Equal(["k8s_pods"], asked.Options!.Tools!.Select(t => t.Name));
        var routed = (await Drain(channel)).OfType<ModelRouted>().Select(r => string.Join(",", r.Attempts.Select(a => $"{a.Candidate}:{a.Outcome}")));
        Assert.Equal(["frontier:Answered,local:NotTried", "frontier:Failed,local:Answered"], routed);
        Assert.Equal(1, route.Health.Snapshot().Single(e => e.Key == "frontier").Failures);
    }

    [Fact]
    public async Task A_Model_Node_Over_A_Route_Moves_Past_A_Refusal()
    {
        var careful = new ScriptedChatClient(new Scripted(["I can't help with that."], FinishReason: "content_filter"));
        var plain = new ScriptedChatClient(Scripted.Text("restart the pod"));
        var channel = new RunEventChannel();

        var result = await OneCall(Route("refusals", Candidate("careful", careful), Candidate("plain", plain)))
            .RunAsync("refused", new Job("fix?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.Equal("restart the pod", Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Answer);
        var routed = (await Drain(channel)).OfType<ModelRouted>().Single();
        Assert.Equal([ModelRouteOutcome.Refused, ModelRouteOutcome.Answered], routed.Attempts.Select(a => a.Outcome));
        Assert.Equal("plain", routed.Answered);
    }

    [Fact]
    public async Task A_Streaming_Run_Through_A_Hedged_Route_Streams_Only_The_Winners_Tokens()
    {
        var slow = new ScriptedChatClient(new Scripted(["slow ", "answer"]) { Hang = true });
        var fast = new ScriptedChatClient(Scripted.Text("Roll ", "back."));
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "hedged",
            Candidates = [Candidate("slow", slow), Candidate("fast", fast)],
            HedgeAfter = TimeSpan.FromMilliseconds(30),
        });
        var channel = new RunEventChannel();

        var (reader, completion) = OneCall(route).RunStreaming("streamed-route", new Job("fix?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });
        var tokens = new List<string>();
        await foreach (var chunk in reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            if (chunk.Type == StreamChunkType.Token)
            {
                tokens.Add(chunk.Payload.ToString());
            }
        }

        Assert.Equal("Roll back.", Assert.IsType<GraphExecutionResult<Job>.Completed>(await completion).State.Answer);
        Assert.Equal(["Roll ", "back."], tokens);
        Assert.True(Assert.Single(slow.Requests).Streamed);
        var events = await Drain(channel);
        var routed = events.OfType<ModelRouted>().Single();
        Assert.Equal([ModelRouteOutcome.LostRace, ModelRouteOutcome.Answered], routed.Attempts.Select(a => a.Outcome));
        Assert.Equal(1, routed.Hedges);
        Assert.Equal(SlowThenFast.Order(), events.OfType<ModelCallCompleted>().Select(c => c.Result).Order());
    }

    [Fact]
    public async Task An_Exhausted_Route_Fails_The_Node_Transiently_And_Its_Hint_Is_The_Retry_Delay()
    {
        var clock = new FakeTimeProvider();
        var limited = new ScriptedChatClient(
            Scripted.Fails(new ModelCallException("429 Too Many Requests", isTransient: true, statusCode: 429, retryAfter: TimeSpan.FromSeconds(4))),
            Scripted.Text("ok"));
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "limited",
            Candidates = [Candidate("only", limited)],
            Health = new ModelHealth(new ModelHealthOptions { TimeProvider = clock }),
        });
        var channel = new RunEventChannel();
        var policy = RecoveryPolicy.Retry(1).WithBackoff(TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(30));

        var run = OneCall(route, policy).RunAsync(
            "retried", new Job("?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel, TimeProvider = clock });
        for (var i = 0; i < 10_000 && !run.IsCompleted; i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(500));
            await Task.Delay(1);
        }

        Assert.Equal("ok", Assert.IsType<GraphExecutionResult<Job>.Completed>(await run.WaitAsync(Patience)).State.Answer);
        var events = await Drain(channel);
        var retry = events.OfType<RetryScheduled>().Single();
        Assert.Equal(TimeSpan.FromSeconds(4), retry.Delay);
        Assert.Equal("model call failed (route_exhausted): no candidate of route 'limited' answered: only failed (429)", retry.Error);
        Assert.Equal(
            [ModelRouteOutcome.Failed, ModelRouteOutcome.Answered],
            events.OfType<ModelRouted>().Select(r => Assert.Single(r.Attempts).Outcome));
    }

    [Fact]
    public async Task A_Route_Outside_A_Node_Routes_Opens_Spans_And_Calls_On_Routed()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = Spans(spans);
        var results = new ConcurrentQueue<ModelRouteResult>();
        var down = new ScriptedChatClient(Scripted.Fails(new HttpRequestException("connection refused")), Scripted.Fails(new HttpRequestException("connection refused")));
        var up = new ScriptedChatClient(Scripted.Text("one"), Scripted.Text("two"));
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "outside",
            Candidates = [Candidate("down", down), Candidate("up", up)],
            OnRouted = results.Enqueue,
        });

        var plain = await route.GetResponseAsync([new(ChatRole.User, "hi")]);
        var through = await StrataChat.GetResponseAsync(route, [new(ChatRole.User, "hi")]);

        Assert.Equal(["one", "two"], [plain.Text, through.Text]);
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("up", r.Answered));
        var routeSpans = spans.Where(a => a.OperationName == "strata.route" && a.DisplayName == "route outside").ToList();
        Assert.Equal(2, routeSpans.Count);
        Assert.All(routeSpans, s => Assert.Null(s.GetTagItem("strata.thread_id")));
        Assert.Equal(4, spans.Count(a => a.OperationName == "strata.model" && routeSpans.Any(r => r.SpanId == a.ParentSpanId)));
    }

    /// <summary>
    /// A loser isn't waited for, so its model call completes when it notices its token: after the winner's, perhaps.
    /// </summary>
    private static readonly ModelCallResult[] SlowThenFast = [ModelCallResult.Cancelled, ModelCallResult.Succeeded];

    [Fact]
    public async Task A_Hedge_Winner_Does_Not_Wait_For_A_Loser_That_Ignores_Its_Token()
    {
        var results = new ConcurrentQueue<ModelRouteResult>();
        var stubborn = new Stubborn(TimeSpan.FromSeconds(2));
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "stubborn",
            Candidates = [Candidate("stubborn", stubborn), Candidate("quick", new ScriptedChatClient(Scripted.Text("quick")))],
            HedgeAfter = TimeSpan.FromMilliseconds(50),
            OnRouted = results.Enqueue,
        });
        var watch = Stopwatch.StartNew();

        var response = await route.GetResponseAsync([new(ChatRole.User, "hi")]);

        watch.Stop();
        Assert.Equal("quick", response.Text);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(1.5), $"the winner waited {watch.Elapsed} for its loser");
        var routed = Assert.Single(results);
        Assert.Equal([ModelRouteOutcome.LostRace, ModelRouteOutcome.Answered], routed.Attempts.Select(a => a.Outcome));
        // The loser ends on its own, later, and is reaped then.
        await stubborn.Ended.Task.WaitAsync(Patience);
    }

    [Fact]
    public async Task A_Throwing_Classifier_On_A_Half_Open_Test_Still_Lets_The_Key_Be_Tested_Again()
    {
        var clock = new FakeTimeProvider();
        var client = new ScriptedChatClient(
            Scripted.Fails(new HttpRequestException("connection refused")),
            Scripted.Fails(new InvalidOperationException("odd")),
            Scripted.Text("back"));
        var results = new ConcurrentQueue<ModelRouteResult>();
        var health = new ModelHealth(new ModelHealthOptions { Breaker = new ModelBreaker(1, TimeSpan.FromSeconds(10)), TimeProvider = clock });
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "probe",
            Candidates = [Candidate("a", client)],
            Health = health,
            FaultClassifier = new Throwing(),
            OnRouted = results.Enqueue,
        });
        List<ChatMessage> hi = [new(ChatRole.User, "hi")];

        // The classifier throws on every fault: the default reads them instead.
        var exhausted = await Assert.ThrowsAsync<ModelRouteException>(() => route.GetResponseAsync(hi));
        Assert.Equal("no candidate of route 'probe' answered: a failed (connection)", exhausted.Message);
        clock.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(ModelHealthState.HalfOpen, Assert.Single(health.Snapshot()).State);
        await Assert.ThrowsAsync<InvalidOperationException>(() => route.GetResponseAsync(hi));
        var answered = await route.GetResponseAsync(hi);

        Assert.Equal("back", answered.Text);
        var tested = results.ElementAt(1).Attempts.Single();
        Assert.Equal(ModelRouteOutcome.Failed, tested.Outcome);
        Assert.NotNull(tested.CallId);
        Assert.Equal(new ModelHealthEntry("a", ModelHealthState.Available, 0, null), Assert.Single(health.Snapshot()));
    }

    private sealed class Throwing : IModelFaultClassifier
    {
        public ModelFault? Classify(Exception exception) => throw new InvalidOperationException("the classifier is broken");
    }

    [Fact]
    public async Task A_Node_Stream_That_Closes_Fails_The_Call_Transiently_As_Stream_Closed()
    {
        var outcomes = new ConcurrentBag<string>();
        using var meters = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == StrataTelemetry.Name && instrument.Name == "strata.route.calls")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        meters.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            var map = new Dictionary<string, object?>();
            foreach (var tag in tags)
            {
                map[tag.Key] = tag.Value;
            }

            if (Equals(map["strata.route"], "closed-stream"))
            {
                outcomes.Add((string)map["strata.outcome"]!);
            }
        });
        meters.Start();
        var results = new ConcurrentQueue<ModelRouteResult>();
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "closed-stream",
            Candidates = [Candidate("only", new ScriptedChatClient(Scripted.Text("Roll ", "back.")))],
            OnRouted = results.Enqueue,
        });
        var context = new NodeContext { ThreadId = "t", Node = "Ask", NodePath = "Ask", Stream = new ClosedStream() };

        var failure = await Assert.ThrowsAsync<NodeException>(
            () => StrataChat.GetResponseAsync(route, [new(ChatRole.User, "fix?")], null, context));

        Assert.Equal(NodeFailureKind.Transient, failure.Kind);
        Assert.StartsWith("model call failed (stream_closed): ", failure.Message, StringComparison.Ordinal);
        var attempt = Assert.Single(Assert.Single(results).Attempts);
        Assert.Equal((ModelRouteOutcome.Failed, "stream_closed"), (attempt.Outcome, attempt.Reason));
        Assert.Equal(["failed"], outcomes);
        Assert.Equal(0, Assert.Single(route.Health.Snapshot()).Failures);
    }

    /// <summary>A node's stream whose channel has closed.</summary>
    private sealed class ClosedStream : IStreamContext
    {
        public ValueTask EmitTokenAsync(string token, CancellationToken cancellationToken = default) =>
            ValueTask.FromException(new System.Threading.Channels.ChannelClosedException());

        public ValueTask EmitDiagnosticAsync(StreamChunkType type, string message, CancellationToken cancellationToken = default) =>
            ValueTask.FromException(new System.Threading.Channels.ChannelClosedException());

        public void EmitToken(string token) => throw new System.Threading.Channels.ChannelClosedException();

        public void EmitDiagnostic(StreamChunkType type, string message) => throw new System.Threading.Channels.ChannelClosedException();
    }

    [Fact]
    public async Task With_No_Model_Named_Anywhere_The_Route_Says_Unknown()
    {
        var results = new ConcurrentQueue<ModelRouteResult>();
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "nameless",
            Candidates = [Candidate("only", new Anonymous())],
            OnRouted = results.Enqueue,
        });

        // A blank model is no model: the call goes ahead and names it unknown.
        var response = await route.GetResponseAsync([new(ChatRole.User, "hi")], new ChatOptions { ModelId = " " });

        Assert.Equal("anonymous", response.Text);
        var routed = Assert.Single(results);
        Assert.Equal(("only", "unknown"), (routed.Answered, routed.Model));
    }

    /// <summary>A client that answers without naming a model, and says nothing of itself.</summary>
    private sealed class Anonymous : IChatClient
    {
        public Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
            Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, "anonymous")));

        public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public void Dispose()
        {
        }
    }

    /// <summary>A client that ignores its token and answers after <paramref name="delay"/>.</summary>
    private sealed class Stubborn(TimeSpan delay) : IChatClient
    {
        public TaskCompletionSource Ended { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
        {
            await Task.Delay(delay, CancellationToken.None);
            Ended.TrySetResult();
            return new ChatResponse(new ChatMessage(ChatRole.Assistant, "late"));
        }

        public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public void Dispose()
        {
        }
    }

    [Fact]
    public void The_Fault_Classifier_Reads_A_Route_Failure_First()
    {
        var exhausted = new ModelRouteException("r", "no candidate of route 'r' answered: a failed (429)", true, TimeSpan.FromSeconds(5), "route_exhausted", []);
        var ineligible = new ModelRouteException("r", "no candidate of route 'r' can take this request: a is ineligible (x)", false, null, "route_ineligible", []);

        Assert.Equal(new ModelFault(true, TimeSpan.FromSeconds(5), "route_exhausted"), ModelFaults.Classify(exhausted));
        Assert.Equal(new ModelFault(false, null, "route_ineligible"), ModelFaults.Classify(ineligible, new Everything()));
    }

    private sealed class Everything : IModelFaultClassifier
    {
        public ModelFault? Classify(Exception exception) => new(true, null, "classified");
    }

    [Fact]
    public void A_Route_Refuses_A_Bad_Shape()
    {
        var client = new ScriptedChatClient();

        Assert.Throws<ArgumentException>(() => Route("empty"));
        Assert.Throws<ArgumentException>(() => Route("twice", new ModelCandidate("a", client), new ModelCandidate("a", client)));
        Assert.Throws<ArgumentException>(() => Route(" ", new ModelCandidate("a", client)));
        Assert.ThrowsAny<ArgumentException>(() => new ModelRoute(new ModelRouteOptions
        {
            Name = "r",
            Candidates = [new ModelCandidate("a", client)],
            HedgeAfter = TimeSpan.Zero,
        }));
        Assert.ThrowsAny<ArgumentException>(() => new ModelHealth(new ModelHealthOptions { Breaker = new ModelBreaker(0, TimeSpan.FromSeconds(1)) }));
    }

    [Fact]
    public async Task A_Candidates_Health_Key_Is_Its_Providers_Name_Unless_It_Names_One()
    {
        static Scripted Refused() => Scripted.Fails(new HttpRequestException("connection refused"));
        var route = Route(
            "keys",
            new ModelCandidate("a", new ScriptedChatClient(Refused())),
            new ModelCandidate("b", new ScriptedChatClient(Refused())) { HealthKey = "b-key" },
            new ModelCandidate("c", new Unnamed()));

        await Assert.ThrowsAsync<ModelRouteException>(() => route.GetResponseAsync([new(ChatRole.User, "hi")]));

        Assert.Equal(["b-key", "c", "scripted"], route.Health.Snapshot().Select(e => e.Key));
        Assert.Same(route, ((IChatClient)route).GetService<ModelRoute>());
    }

    /// <summary>A client that says nothing of itself.</summary>
    private sealed class Unnamed : IChatClient
    {
        public Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
            throw new HttpRequestException("connection refused");

        public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public void Dispose()
        {
        }
    }

    /// <summary>A client that calls <paramref name="onAsked"/> before it answers.</summary>
    private sealed class Asked(IChatClient inner, Action onAsked) : IChatClient
    {
        public Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
        {
            onAsked();
            return inner.GetResponseAsync(messages, options, cancellationToken);
        }

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages, ChatOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            onAsked();
            await foreach (var update in inner.GetStreamingResponseAsync(messages, options, cancellationToken))
            {
                yield return update;
            }
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => inner.GetService(serviceType, serviceKey);

        public void Dispose()
        {
        }
    }
}
