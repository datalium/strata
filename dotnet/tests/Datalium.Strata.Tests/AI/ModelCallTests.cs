using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Streaming;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// A model call from a node (<c>design/44-LLM-Integration.md</c>, PR 1): streamed under <c>RunStreaming</c> and not
/// otherwise, observed as <see cref="ModelCallStarted"/> / <see cref="ModelCallCompleted"/> with the provider's usage,
/// failed as the fault table says, the hint reaching the retry. Mirrors <c>rust/strata-ai/tests/model_calls.rs</c>.
/// </summary>
public class ModelCallTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(string Prompt, string Answer = "");

    private static CompiledGraph<Job> OneCall(IChatClient chat, RecoveryPolicy? policy = null)
    {
        var graph = new StateGraph<Job>()
            .AddModelNode<Job>(
                "Ask",
                chat,
                (job, _) => [new(ChatRole.User, job.Prompt)],
                (job, response) => job with { Answer = response.Text },
                new ModelNodeOptions { Chat = new ChatOptions { ModelId = "gpt-test" } });
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
        ModelCallStarted s => $"ModelCallStarted {e.NodePath}#{e.Attempt} {s.Operation} {s.Model} {s.Provider}",
        ModelCallCompleted c => $"ModelCallCompleted {e.NodePath}#{e.Attempt} {c.Operation} {c.Model} {c.Result}"
            + (c.Result == ModelCallResult.Succeeded ? $" {c.Usage.InputTokens}/{c.Usage.OutputTokens} {c.FinishReason}" : ""),
        RetryScheduled r => $"RetryScheduled {e.NodePath}#{e.Attempt}",
        NodeCompleted n => $"NodeCompleted {e.NodePath} {n.Result}",
        _ => e.GetType().Name,
    };

    [Fact]
    public async Task A_Call_Outside_A_Streaming_Run_Is_One_Request_And_Is_Observed()
    {
        var chat = new ScriptedChatClient(Scripted.Text("The cause ", "is a bad deploy."));
        var channel = new RunEventChannel();

        var result = await OneCall(chat).RunAsync("plain", new Job("why?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        var completed = Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        Assert.Equal("The cause is a bad deploy.", completed.State.Answer);
        var request = Assert.Single(chat.Requests);
        Assert.False(request.Streamed);
        Assert.Equal("gpt-test", request.Options?.ModelId);
        var events = (await Drain(channel)).Select(Describe).ToList();
        Assert.Equal(
            [
                "RunStarted", "NodeStarted",
                "ModelCallStarted Ask#1 chat gpt-test scripted",
                "ModelCallCompleted Ask#1 chat gpt-test Succeeded 12/5 stop",
                "NodeCompleted Ask Committed", "RunCompleted",
            ],
            events);
    }

    [Fact]
    public async Task A_Call_In_A_Streaming_Run_Streams_Its_Text_And_Reports_The_Streamed_Usage()
    {
        var chat = new ScriptedChatClient(new Scripted(["Roll ", "back ", "v42."], InputTokens: 30, OutputTokens: 3));
        var channel = new RunEventChannel();

        var (reader, completion) = OneCall(chat).RunStreaming("streamed", new Job("fix?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });
        var tokens = new List<string>();
        await foreach (var chunk in reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            if (chunk.Type == StreamChunkType.Token)
            {
                Assert.Equal("Ask", chunk.NodeName);
                tokens.Add(chunk.Payload.ToString());
            }
        }

        var completed = Assert.IsType<GraphExecutionResult<Job>.Completed>(await completion);
        Assert.Equal("Roll back v42.", completed.State.Answer);
        Assert.Equal(["Roll ", "back ", "v42."], tokens);
        Assert.True(Assert.Single(chat.Requests).Streamed);
        var done = (await Drain(channel)).OfType<ModelCallCompleted>().Single();
        Assert.Equal(new ModelUsage(30, 3), done.Usage);
        Assert.Equal("stop", done.FinishReason);
    }

    [Fact]
    public async Task A_Rate_Limit_Is_Transient_And_Its_Hint_Is_The_Retry_Delay()
    {
        var chat = new ScriptedChatClient(
            Scripted.Fails(new ModelCallException("429 Too Many Requests", isTransient: true, statusCode: 429, retryAfter: TimeSpan.FromMilliseconds(40))),
            Scripted.Text("ok"));
        var channel = new RunEventChannel();
        var policy = RecoveryPolicy.Retry(2).WithBackoff(TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(5));

        var result = await OneCall(chat, policy).RunAsync("limited", new Job("?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        var events = await Drain(channel);
        Assert.Equal(TimeSpan.FromMilliseconds(40), events.OfType<RetryScheduled>().Single().Delay);
        var failed = events.OfType<ModelCallCompleted>().First();
        Assert.Equal(ModelCallResult.Failed, failed.Result);
        Assert.Contains("429", failed.Error);
        Assert.Equal(ModelCallResult.Succeeded, events.OfType<ModelCallCompleted>().Last().Result);
    }

    [Fact]
    public async Task A_Bad_Request_Is_Fatal_And_Is_Not_Retried()
    {
        var chat = new ScriptedChatClient(
            Scripted.Fails(new HttpRequestException("bad request", null, HttpStatusCode.BadRequest)),
            Scripted.Text("never"));

        var result = await OneCall(chat, RecoveryPolicy.Retry(3)).RunAsync("bad", new Job("?"), new InMemoryCheckpointStore());

        var failed = Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result);
        Assert.Equal(1, failed.Attempts);
        Assert.Contains("model call failed (400)", failed.Message);
        Assert.Single(chat.Requests);
    }

    [Theory]
    [InlineData(408, true)]
    [InlineData(409, true)]
    [InlineData(429, true)]
    [InlineData(500, true)]
    [InlineData(503, true)]
    [InlineData(529, true)]
    [InlineData(400, false)]
    [InlineData(401, false)]
    [InlineData(403, false)]
    [InlineData(404, false)]
    [InlineData(422, false)]
    public void Each_Status_Is_Classed_As_The_Fault_Table_Says(int status, bool transient)
    {
        var fault = ModelFaults.Classify(new HttpRequestException("x", null, (HttpStatusCode)status));

        Assert.Equal(transient, fault.IsTransient);
        Assert.Equal(status.ToString(System.Globalization.CultureInfo.InvariantCulture), fault.ErrorType);
    }

    [Fact]
    public void Timeouts_And_Lost_Connections_Are_Transient_And_Anything_Unknown_Is_Fatal()
    {
        Assert.Equal(new ModelFault(true, null, "timeout"), ModelFaults.Classify(new TaskCanceledException("client timeout")));
        Assert.Equal(new ModelFault(true, null, "timeout"), ModelFaults.Classify(new TimeoutException()));
        Assert.Equal(new ModelFault(true, null, "connection"), ModelFaults.Classify(new HttpRequestException("reset")));
        Assert.Equal(new ModelFault(true, null, "connection"), ModelFaults.Classify(new IOException("reset")));
        var unknown = ModelFaults.Classify(new InvalidOperationException("no"));
        Assert.False(unknown.IsTransient);
        Assert.Equal("System.InvalidOperationException", unknown.ErrorType);
    }

    [Fact]
    public void A_Classifier_Reads_A_Providers_Own_Exception_Before_The_Default()
    {
        var classifier = new OverloadedClassifier();

        var fault = ModelFaults.Classify(new InvalidOperationException("overloaded"), classifier);

        Assert.Equal(new ModelFault(true, TimeSpan.FromSeconds(3), "529"), fault);
    }

    private sealed class OverloadedClassifier : IModelFaultClassifier
    {
        public ModelFault? Classify(Exception exception) =>
            exception.Message == "overloaded" ? new ModelFault(true, TimeSpan.FromSeconds(3), "529") : null;
    }

    [Theory]
    [InlineData("1500", null, 1500)]
    [InlineData(null, "2", 2000)]
    [InlineData(null, "0.5", 500)]
    [InlineData("250", "9", 250)]
    public void Retry_After_Reads_Milliseconds_Then_Seconds(string? ms, string? seconds, int expectedMs)
    {
        Assert.Equal(TimeSpan.FromMilliseconds(expectedMs), RetryAfter.Parse(ms, seconds, DateTimeOffset.UtcNow));
    }

    [Fact]
    public void Retry_After_Reads_An_Http_Date_And_Nothing_Else()
    {
        var now = new DateTimeOffset(2026, 10, 21, 12, 0, 0, TimeSpan.Zero);

        Assert.Equal(TimeSpan.FromSeconds(30), RetryAfter.Parse(null, "Wed, 21 Oct 2026 12:00:30 GMT", now));
        Assert.Equal(TimeSpan.Zero, RetryAfter.Parse(null, "Wed, 21 Oct 2026 11:00:00 GMT", now));
        Assert.Null(RetryAfter.Parse(null, "soon", now));
        Assert.Null(RetryAfter.Parse(null, null, now));
        Assert.Null(RetryAfter.Parse(null, "-3", now));
    }

    [Fact]
    public async Task The_Handler_Turns_A_Retryable_Response_Into_A_Fault_With_Its_Hint_And_Passes_Others()
    {
        using var limited = new HttpClient(new RetryAfterHandler(new Answering(HttpStatusCode.TooManyRequests, "retry-after-ms", "1500")));
        using var refused = new HttpClient(new RetryAfterHandler(new Answering(HttpStatusCode.BadRequest, "Retry-After", "9")));

        var fault = await Assert.ThrowsAsync<ModelCallException>(() => limited.GetAsync("http://model.test/v1/chat"));
        using var passed = await refused.GetAsync("http://model.test/v1/chat");

        Assert.True(fault.IsTransient);
        Assert.Equal(429, fault.StatusCode);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), fault.RetryAfter);
        Assert.Contains("slow down", fault.Message);
        Assert.Equal(HttpStatusCode.BadRequest, passed.StatusCode);
    }

    private sealed class Answering(HttpStatusCode status, string header, string value) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = new HttpResponseMessage(status) { Content = new StringContent("{\"error\":\"slow down\"}") };
            response.Headers.TryAddWithoutValidation(header, value);
            return Task.FromResult(response);
        }
    }

    [Fact]
    public async Task A_Stream_That_Breaks_After_It_Started_Is_Retried_And_The_Stream_Says_So()
    {
        var chat = new ScriptedChatClient(
            new Scripted(["Half "]) { Failure = new InvalidOperationException("socket closed"), FailAfter = 1 },
            Scripted.Text("Whole answer."));

        var (reader, completion) = OneCall(chat, RecoveryPolicy.Retry(1)).RunStreaming("broken", new Job("?"), new InMemoryCheckpointStore());
        var chunks = new List<string>();
        await foreach (var chunk in reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            if (chunk.Type is StreamChunkType.Token or StreamChunkType.Retry)
            {
                chunks.Add($"{chunk.Type}:{(chunk.Type == StreamChunkType.Token ? chunk.Payload.ToString() : "")}");
            }
        }

        var completed = Assert.IsType<GraphExecutionResult<Job>.Completed>(await completion);
        Assert.Equal("Whole answer.", completed.State.Answer);
        Assert.Equal(["Token:Half ", "Retry:", "Token:Whole answer."], chunks);
    }

    [Fact]
    public async Task A_Client_Failing_With_A_Node_Exception_Fails_The_Node_As_It_Says()
    {
        var chat = new ScriptedChatClient(Scripted.Fails(NodeException.Fatal("budget exhausted")));

        var result = await OneCall(chat, RecoveryPolicy.Retry(2)).RunAsync("budget", new Job("?"), new InMemoryCheckpointStore());

        var failed = Assert.IsType<GraphExecutionResult<Job>.NodeFailed>(result);
        Assert.Equal("budget exhausted", failed.Message);
        Assert.Equal(1, failed.Attempts);
    }

    [Fact]
    public async Task A_Cancelled_Run_Stops_The_Call_And_Ends_Cancelled()
    {
        var chat = new ScriptedChatClient(new Scripted([]) { Hang = true });
        using var cts = new CancellationTokenSource();

        var run = OneCall(chat).RunAsync("cancel", new Job("?"), new InMemoryCheckpointStore(), cts.Token);
        while (chat.Requests.IsEmpty)
        {
            await Task.Delay(5);
        }

        await cts.CancelAsync();

        Assert.IsType<GraphExecutionResult<Job>.Cancelled>(await run.WaitAsync(Patience));
    }

    /// <summary>
    /// A .NET node the engine gives up on is abandoned, so a call it was making can end after <c>RunCompleted</c>,
    /// whose events are dropped (as design/20 says of tool calls). Cancelling the call from inside the node fixes
    /// the order, as <c>ToolMiddlewareTests</c> does for a tool call.
    /// </summary>
    [Fact]
    public async Task A_Cancelled_Call_Is_Completed_As_Cancelled()
    {
        var chat = new ScriptedChatClient(new Scripted([]) { Hang = true });
        var channel = new RunEventChannel();
        var graph = new StateGraph<Job>()
            .AddNode("Ask", async (job, ctx, ct) =>
            {
                using var call = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var asking = StrataChat.GetResponseAsync(chat, [new(ChatRole.User, job.Prompt)], null, ctx, null, call.Token);
                while (chat.Requests.IsEmpty)
                {
                    await Task.Delay(5, ct);
                }

                await call.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => asking);
                return job with { Answer = "gave up" };
            })
            .Compile();

        var result = await graph.RunAsync("cancel-call", new Job("?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        var done = (await Drain(channel)).OfType<ModelCallCompleted>().Single();
        Assert.Equal(ModelCallResult.Cancelled, done.Result);
        Assert.Equal(ScriptedChatClient.Model, done.Model);
        Assert.Equal(ModelUsage.None, done.Usage);
    }

    [Fact]
    public void The_Model_Node_Exports_As_A_Node_With_Its_Metadata()
    {
        var graph = new StateGraph<Job>()
            .AddModelNode<Job>(
                "Summarize",
                new ScriptedChatClient(),
                (job, _) => [new(ChatRole.User, job.Prompt)],
                (job, response) => job with { Answer = response.Text },
                new ModelNodeOptions { Metadata = new Dictionary<string, string> { ["agent"] = "summarizer" } })
            .Compile();

        var node = Assert.Single(graph.ToDefinition().Nodes);

        Assert.Equal("Summarize", node.Name);
        Assert.Equal("summarizer", node.Metadata!["agent"]);
    }

    [Fact]
    public async Task The_Span_Carries_The_GenAI_Attributes_And_The_Metrics_Record_Usage()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var usage = new ConcurrentBag<(long Value, Dictionary<string, object?> Tags)>();
        using var meters = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == StrataTelemetry.Name && instrument.Name == "gen_ai.client.token.usage")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        meters.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            var map = new Dictionary<string, object?>();
            foreach (var tag in tags)
            {
                map[tag.Key] = tag.Value;
            }

            usage.Add((value, map));
        });
        meters.Start();
        var chat = new ScriptedChatClient(new Scripted(["ok"], InputTokens: 101, OutputTokens: 7));

        await OneCall(chat).RunAsync("span-thread", new Job("?"), new InMemoryCheckpointStore());

        var span = spans.Single(a => a.OperationName == "strata.model" && (string?)a.GetTagItem("strata.thread_id") == "span-thread");
        Assert.Equal("chat gpt-test", span.DisplayName);
        Assert.Equal(ActivityKind.Client, span.Kind);
        Assert.Equal("chat", span.GetTagItem("gen_ai.operation.name"));
        Assert.Equal("scripted", span.GetTagItem("gen_ai.provider.name"));
        Assert.Equal("gpt-test", span.GetTagItem("gen_ai.request.model"));
        Assert.Equal("gpt-test", span.GetTagItem("gen_ai.response.model"));
        Assert.Equal(101L, span.GetTagItem("gen_ai.usage.input_tokens"));
        Assert.Equal(7L, span.GetTagItem("gen_ai.usage.output_tokens"));
        Assert.Equal(new[] { "stop" }, span.GetTagItem("gen_ai.response.finish_reasons"));
        Assert.Equal("Ask", span.GetTagItem("strata.node.path"));
        var node = spans.Single(a => a.TraceId == span.TraceId && a.OperationName == "strata.attempt");
        Assert.Equal(node.SpanId, span.ParentSpanId);
        var mine = usage.Where(m => Equals(m.Tags.GetValueOrDefault("gen_ai.request.model"), "gpt-test") && m.Value is 101 or 7).ToList();
        Assert.Contains(mine, m => m.Value == 101 && Equals(m.Tags["gen_ai.token.type"], "input"));
        Assert.Contains(mine, m => m.Value == 7 && Equals(m.Tags["gen_ai.token.type"], "output"));
    }

    [Fact]
    public async Task A_Call_Outside_A_Run_Opens_A_Span_And_Records_No_Event()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var chat = new ScriptedChatClient(Scripted.Text("standalone"));

        var response = await StrataChat.GetResponseAsync(chat, [new(ChatRole.User, "hi")], new ChatOptions { ModelId = "outside-model" });

        Assert.Equal("standalone", response.Text);
        Assert.False(Assert.Single(chat.Requests).Streamed);
        var span = spans.Single(a => (string?)a.GetTagItem("gen_ai.request.model") == "outside-model");
        Assert.Null(span.GetTagItem("strata.thread_id"));
    }

    /// <summary>
    /// The operation names the call (<c>design/45-Long-Term-Memory.md</c>, "Telemetry"): an embedding call's scope is an
    /// <c>embeddings &lt;model&gt;</c> span, tagged so, and its two events say so.
    /// </summary>
    [Fact]
    public async Task A_Scope_Started_For_Another_Operation_Names_Its_Span_And_Its_Events_With_It()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var channel = new RunEventChannel();
        var graph = new StateGraph<Job>()
            .AddNode("Embed", (job, ctx, _) =>
            {
                using var scope = ModelCallScope.Start(ctx, new ModelCallInfo("embed-op-test", "scripted") { Operation = "embeddings" });
                scope.Succeeded(new ModelUsage(4, null), null);
                return ValueTask.FromResult(job);
            })
            .Compile();

        await graph.RunAsync("operation-thread", new Job("?"), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        var events = await Drain(channel);
        Assert.Equal("embeddings", events.OfType<ModelCallStarted>().Single().Operation);
        var completed = events.OfType<ModelCallCompleted>().Single();
        Assert.Equal(("embeddings", new ModelUsage(4, null)), (completed.Operation, completed.Usage));
        var span = spans.Single(a => (string?)a.GetTagItem("gen_ai.request.model") == "embed-op-test");
        Assert.Equal("embeddings embed-op-test", span.DisplayName);
        Assert.Equal("embeddings", span.GetTagItem("gen_ai.operation.name"));
    }

    [Fact]
    public void A_Scope_Records_Its_Outcome_Once()
    {
        using var scope = ModelCallScope.Start(null, new ModelCallInfo("m"));

        scope.Succeeded(new ModelUsage(1, 2), "stop");
        scope.Failed("500", "late");
        scope.Cancelled();

        Assert.Matches("^[0-9a-f-]{36}$", scope.CallId);
        Assert.Throws<ArgumentException>(() => ModelCallScope.Start(null, new ModelCallInfo(" ")));
    }
}
