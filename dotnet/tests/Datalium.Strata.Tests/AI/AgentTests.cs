using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Schema;
using Datalium.Strata.Streaming;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// The agent (<c>design/44-LLM-Integration.md</c>, PR 2) beyond the shared cases (<see cref="AgentCaseTests"/>): what
/// middleware sees, sequential and concurrent tool calls, the requests each turn sends, streaming, cancellation, the
/// agent node, a generated tool's own argument failure, and how a tool's exceptions fail the node. Mirrors
/// <c>rust/strata-ai/tests/agent.rs</c>.
/// </summary>
public sealed class AgentTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(string Answer = "", int Turns = 0);

    public sealed record Diagnosis(string Cause, double Confidence, IReadOnlyList<string> Evidence, string? Severity);

    private static readonly Func<Job, NodeContext, IEnumerable<ChatMessage>> Prompt =
        (_, _) => [new(ChatRole.System, "Investigate."), new(ChatRole.User, "Why?")];

    private static CompiledGraph<Job> AgentNode<TOutput>(StrataAgent<TOutput> agent, RecoveryPolicy? policy = null)
    {
        var graph = new StateGraph<Job>().AddAgentNode(
            "Investigate", agent, Prompt, (job, result) => job with { Answer = $"{result.Output}", Turns = result.Turns });
        if (policy is not null)
        {
            graph.SetRecoveryPolicy("Investigate", policy);
        }

        return graph.Compile();
    }

    private static StrataAgentOptions<T> Options<T>(string id = "a") => new() { Id = id };

    /// <summary>Records what each call's context says.</summary>
    private sealed class Witness : IToolMiddleware
    {
        public ConcurrentQueue<string> Seen { get; } = [];

        public Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            var c = call.Context;
            Seen.Enqueue($"{call.Name} caller={c.CallerId} call={c.CallId} node={c.NodePath} attempt={c.Attempt}");
            return next(call, cancellationToken);
        }
    }

    [Fact]
    public async Task Middleware_Sees_The_Strata_Name_The_Agent_As_Caller_And_The_Models_Call_Id()
    {
        var witness = new Witness();
        var registry = new ToolRegistry().Register(new AgentTools()).Use(witness);
        var chat = new ScriptedChatClient(
            Scripted.Tools(("toolu_01", "k8s_pods", """{"ns":"shop"}""")),
            Scripted.Text("done"));
        var agent = new StrataAgent<string>(chat, registry, Options<string>("kubernetes-investigator"));

        var result = await AgentNode(agent).RunAsync("witnessed", new Job(), new InMemoryCheckpointStore());

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        Assert.Equal(["k8s.pods caller=kubernetes-investigator call=toolu_01 node=Investigate attempt=1"], witness.Seen);
    }

    [Fact]
    public async Task A_Generated_Tool_That_Cant_Read_Its_Arguments_Is_Refused_Back_To_The_Model()
    {
        var registry = new ToolRegistry().Register(new AgentTools());
        var chat = new ScriptedChatClient(
            Scripted.Tools(("1", "k8s_pods", """{"ns":5}""")),
            Scripted.Tools(("2", "k8s_pods", """{"ns":"shop"}""")),
            Scripted.Text("done"));
        var agent = new StrataAgent<string>(chat, registry, Options<string>());
        AgentResult<string>? produced = null;
        var graph = new StateGraph<Job>().AddAgentNode("Investigate", agent, Prompt, (job, r) => { produced = r; return job; }).Compile();

        await graph.RunAsync("refused", new Job(), new InMemoryCheckpointStore());

        var results = produced!.Messages.Where(m => m.Role == ChatRole.Tool)
            .Select(m => (string)m.Contents.OfType<FunctionResultContent>().Single().Result!).ToList();
        Assert.Equal("{\"error\":\"invalid arguments for tool 'k8s_pods': argument 'ns' must be a string\"}", results[0]);
        Assert.Equal("""{"result":"3 pods in shop"}""", results[1]);
        Assert.Equal([ToolCallResult.Refused, ToolCallResult.Succeeded], produced.ToolCalls.Select(c => c.Result));
    }

    /// <summary>Notes how many calls are in flight; <c>a</c> answers only once <c>b</c> has started when asked to wait.</summary>
    private sealed class Gates(bool waiting) : IStrataToolProvider
    {
        private readonly TaskCompletionSource _bStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Lock _gate = new();
        private int _inFlight;
        private int _most;

        public int Most
        {
            get
            {
                lock (_gate)
                {
                    return _most;
                }
            }
        }

        public string GetToolsManifest() =>
            """[{"name":"a","description":"","parameters":{"type":"object","properties":{},"required":[]}},{"name":"b","description":"","parameters":{"type":"object","properties":{},"required":[]}}]""";

        public Task<string> DispatchToolAsync(string toolName, string jsonArguments) =>
            DispatchToolAsync(toolName, jsonArguments, ToolCallContext.None);

        public async Task<string> DispatchToolAsync(
            string toolName, string jsonArguments, ToolCallContext context, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                _most = Math.Max(_most, ++_inFlight);
            }

            if (toolName == "b")
            {
                _bStarted.TrySetResult();
            }

            if (toolName == "a" && waiting)
            {
                await _bStarted.Task.WaitAsync(cancellationToken);
            }
            else
            {
                await Task.Delay(20, cancellationToken);
            }

            lock (_gate)
            {
                _inFlight--;
            }

            return toolName;
        }
    }

    [Fact]
    public async Task A_Turns_Calls_Run_One_At_A_Time_By_Default()
    {
        var gates = new Gates(waiting: false);
        var chat = new ScriptedChatClient(Scripted.Tools(("1", "a", "{}"), ("2", "b", "{}")), Scripted.Text("done"));
        var agent = new StrataAgent<string>(chat, new ToolRegistry().Register(gates), Options<string>());

        await AgentNode(agent).RunAsync("sequential", new Job(), new InMemoryCheckpointStore());

        Assert.Equal(1, gates.Most);
    }

    [Fact]
    public async Task With_Tool_Concurrency_A_Turns_Calls_Run_At_Once_And_Answer_In_The_Models_Order()
    {
        var gates = new Gates(waiting: true);
        var chat = new ScriptedChatClient(Scripted.Tools(("1", "a", "{}"), ("2", "b", "{}")), Scripted.Text("done"));
        var agent = new StrataAgent<string>(chat, new ToolRegistry().Register(gates), Options<string>() with { ToolConcurrency = 2 });
        AgentResult<string>? produced = null;
        var graph = new StateGraph<Job>().AddAgentNode("Investigate", agent, Prompt, (job, r) => { produced = r; return job; }).Compile();

        var result = await graph.RunAsync("concurrent", new Job(), new InMemoryCheckpointStore()).WaitAsync(Patience);

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        Assert.Equal(2, gates.Most);
        Assert.Equal(["1", "2"], produced!.ToolCalls.Select(c => c.CallId));
    }

    [Fact]
    public async Task Every_Turn_Sends_The_Declarations_And_A_Typed_Agent_Asks_For_Its_Schema()
    {
        var registry = new ToolRegistry().Register(new AgentTools());
        var chat = new ScriptedChatClient(
            Scripted.Tools(("1", "k8s_pods", """{"ns":"shop"}""")),
            Scripted.Text("""{"cause":"pods","confidence":0.5,"evidence":["1"],"severity":null}"""));
        var agent = new StrataAgent<Diagnosis>(chat, registry, new StrataAgentOptions<Diagnosis>
        {
            Id = "a",
            Chat = new ChatOptions { ModelId = "gpt-test", MaxOutputTokens = 500 },
        });
        AgentResult<Diagnosis>? produced = null;
        var graph = new StateGraph<Job>().AddAgentNode("Investigate", agent, Prompt, (job, r) => { produced = r; return job; }).Compile();

        await graph.RunAsync("typed", new Job(), new InMemoryCheckpointStore());

        var (cause, confidence, evidence, severity) = produced!.Output;
        Assert.Equal(("pods", 0.5, null), (cause, confidence, severity));
        Assert.Equal(["1"], evidence);
        Assert.Equal(2, chat.Requests.Count);
        foreach (var (_, options, _) in chat.Requests)
        {
            Assert.Equal("gpt-test", options!.ModelId);
            Assert.Equal(500, options.MaxOutputTokens);
            var tool = Assert.IsAssignableFrom<AIFunctionDeclaration>(Assert.Single(options.Tools!));
            Assert.Equal("k8s_pods", tool.Name);
            Assert.Equal("string", tool.JsonSchema.GetProperty("properties").GetProperty("ns").GetProperty("type").GetString());
            var format = Assert.IsType<ChatResponseFormatJson>(options.ResponseFormat);
            Assert.Equal("Diagnosis", format.SchemaName);
            var schema = JsonNode.Parse(format.Schema!.Value.GetRawText())!;
            Assert.False(schema["additionalProperties"]!.GetValue<bool>());
            Assert.Equal(["cause", "confidence", "evidence", "severity"], schema["required"]!.AsArray().Select(n => n!.GetValue<string>()));
        }

        Assert.Equal(4, chat.Requests.Last().Messages.Count); // the prompt, the call, its result
        Assert.Null(new StrataAgent<string>(chat, new ToolRegistry(), Options<string>()).ResponseFormat);
    }

    [Fact]
    public async Task Under_RunStreaming_Every_Turn_Streams_Its_Text()
    {
        var chat = new ScriptedChatClient(
            Scripted.Text("Checking", " pods.").WithToolCall("1", "k8s_pods", """{"ns":"shop"}"""),
            Scripted.Text("Roll ", "back."));
        var agent = new StrataAgent<string>(chat, new ToolRegistry().Register(new AgentTools()), Options<string>());

        var (reader, completion) = AgentNode(agent).RunStreaming("streamed", new Job(), new InMemoryCheckpointStore());
        var tokens = new List<string>();
        await foreach (var chunk in reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            if (chunk.Type == StreamChunkType.Token)
            {
                tokens.Add(chunk.Payload.ToString());
            }
        }

        var completed = Assert.IsType<GraphExecutionResult<Job>.Completed>(await completion);
        Assert.Equal("Roll back.", completed.State.Answer);
        Assert.Equal(["Checking", " pods.", "Roll ", "back."], tokens);
        Assert.All(chat.Requests, r => Assert.True(r.Streamed));
    }

    [Fact]
    public async Task Cancelling_The_Run_While_A_Tool_Runs_Ends_The_Node_Cancelled()
    {
        var chat = new ScriptedChatClient(Scripted.Tools(("1", "events.hang", "{}")));
        var agent = new StrataAgent<string>(chat, new ToolRegistry().Register(new EventTools()), Options<string>());
        using var cts = new CancellationTokenSource();
        var channel = new RunEventChannel();
        var run = AgentNode(agent).RunAsync("cancelled", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel }, cts.Token);
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            if (e is ToolCallStarted)
            {
                await cts.CancelAsync();
                break;
            }
        }

        Assert.IsType<GraphExecutionResult<Job>.Cancelled>(await run.WaitAsync(Patience));
    }

    [Fact]
    public async Task The_Agent_Node_Is_An_Ordinary_Node_And_Folds_The_Result_Into_The_State()
    {
        var agent = new StrataAgent<string>(new ScriptedChatClient(Scripted.Text("ok")), new ToolRegistry(), Options<string>());
        var graph = AgentNode(agent);

        var definition = JsonSerializer.SerializeToNode(graph.ToDefinition())!;
        Assert.Equal("Investigate", definition["nodes"]![0]!["name"]!.GetValue<string>());
        Assert.Null(definition["nodes"]![0]!["kind"]);
        var completed = Assert.IsType<GraphExecutionResult<Job>.Completed>(await graph.RunAsync("node", new Job(), new InMemoryCheckpointStore()));
        Assert.Equal(1, completed.State.Turns);
    }

    /// <summary>Fails as it is told.</summary>
    private sealed class Failing(Exception failure) : IStrataToolProvider
    {
        public string GetToolsManifest() =>
            """[{"name":"flaky","description":"","parameters":{"type":"object","properties":{},"required":[]}}]""";

        public Task<string> DispatchToolAsync(string toolName, string jsonArguments) => throw failure;
    }

    [Fact]
    public async Task A_Tools_Own_NodeException_Keeps_Its_Hint_And_A_Retry_Starts_The_Conversation_Over()
    {
        var chat = new ScriptedChatClient(
            Scripted.Tools(("1", "flaky", "{}")),
            Scripted.Text("after the retry"));
        var agent = new StrataAgent<string>(
            chat,
            new ToolRegistry().Register(new Failing(NodeException.Transient("the backend is busy", TimeSpan.FromMilliseconds(40)))),
            Options<string>());
        var channel = new RunEventChannel();

        var result = await AgentNode(agent, RecoveryPolicy.Retry(1).WithBackoff(TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(5)))
            .RunAsync("hinted", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        var completed = Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        Assert.Equal("after the retry", completed.State.Answer);
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        Assert.Equal(TimeSpan.FromMilliseconds(40), events.OfType<RetryScheduled>().Single().Delay);
        var retried = chat.Requests.Last().Messages;
        Assert.Equal(2, retried.Count);
        Assert.DoesNotContain(retried, m => m.Role == ChatRole.Tool);
    }

    [Fact]
    public async Task A_Tools_Own_ArgumentException_Fails_The_Node_Rather_Than_Being_Refused_As_Bad_Arguments()
    {
        NodeException? failure = null;
        var agent = new StrataAgent<string>(
            new ScriptedChatClient(Scripted.Tools(("1", "flaky", "{}"))),
            new ToolRegistry().Register(new Failing(new ArgumentOutOfRangeException("amount", "a refund must be positive"))),
            Options<string>());
        var graph = new StateGraph<Job>().AddFallibleNode("Investigate", async (job, context, ct) =>
        {
            try
            {
                await agent.RunAsync(Prompt(job, context), context, ct);
                return job;
            }
            catch (NodeException ex)
            {
                failure = ex;
                throw;
            }
        }).Compile();

        await graph.RunAsync("own-argument", new Job(), new InMemoryCheckpointStore());

        Assert.Equal(NodeFailureKind.Fatal, failure!.Kind);
        Assert.StartsWith("tool 'flaky' failed: a refund must be positive", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Under_ReturnToModel_A_Tools_Own_ArgumentException_Goes_Back_As_A_Failure()
    {
        var chat = new ScriptedChatClient(Scripted.Tools(("1", "flaky", "{}")), Scripted.Text("noted"));
        var agent = new StrataAgent<string>(
            chat,
            new ToolRegistry().Register(new Failing(new ArgumentException("no such order"))),
            Options<string>() with { ToolFailures = ToolFailures.ReturnToModel });
        AgentResult<string>? produced = null;
        var graph = new StateGraph<Job>().AddAgentNode("Investigate", agent, Prompt, (job, r) => { produced = r; return job; }).Compile();

        await graph.RunAsync("own-argument-returned", new Job(), new InMemoryCheckpointStore());

        Assert.Equal([ToolCallResult.Failed], produced!.ToolCalls.Select(c => c.Result));
        var result = (string)produced.Messages.Single(m => m.Role == ChatRole.Tool).Contents.OfType<FunctionResultContent>().Single().Result!;
        Assert.Equal("{\"error\":\"tool 'flaky' failed: no such order\"}", result);
    }

    [Fact]
    public async Task ConfigureTurn_Changes_One_Turns_Options_And_Sees_Where_The_Run_Is()
    {
        var registry = new ToolRegistry().Register(new AgentTools());
        var chat = new ScriptedChatClient(
            Scripted.Tools(("1", "k8s_pods", """{"ns":"shop"}""")),
            Scripted.Text("not json"),
            Scripted.Text("""{"cause":"pods","confidence":0.5,"evidence":["1"],"severity":null}"""));
        var seen = new List<AgentTurn>();
        var agent = new StrataAgent<Diagnosis>(chat, registry, new StrataAgentOptions<Diagnosis>
        {
            Id = "a",
            Chat = new ChatOptions { Temperature = 0.2f },
            ConfigureTurn = (turn, options) =>
            {
                seen.Add(turn with { Conversation = [.. turn.Conversation] });
                Assert.NotNull(options.Tools);
                Assert.NotNull(options.ResponseFormat);
                if (turn.Number == 1)
                {
                    options.ToolMode = ChatToolMode.RequireAny;
                }
            },
        });

        var result = await AgentNode(agent).RunAsync("configured", new Job(), new InMemoryCheckpointStore());

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        Assert.Equal(
            [(1, 0, 0, false, 2), (2, 1, 0, false, 4), (3, 1, 1, true, 6)],
            seen.Select(t => (t.Number, t.ToolTurns, t.Repairs, t.IsRepair, t.Conversation.Count)));
        var requests = chat.Requests.ToList();
        Assert.Equal([ChatToolMode.RequireAny, null, null], requests.Select(r => r.Options!.ToolMode));
        Assert.All(requests, r => Assert.Equal(0.2f, r.Options!.Temperature));
        Assert.Null(agent.Options.Chat!.ToolMode);
    }

    [Fact]
    public async Task A_Tools_Http_Fault_Is_Read_By_The_Fault_Classifier()
    {
        NodeException? failure = null;
        var agent = new StrataAgent<string>(
            new ScriptedChatClient(Scripted.Tools(("1", "flaky", "{}"))),
            new ToolRegistry().Register(new Failing(new HttpRequestException("unavailable", null, HttpStatusCode.ServiceUnavailable))),
            Options<string>());
        var graph = new StateGraph<Job>().AddFallibleNode("Investigate", async (job, context, ct) =>
        {
            try
            {
                await agent.RunAsync(Prompt(job, context), context, ct);
                return job;
            }
            catch (NodeException ex)
            {
                failure = ex;
                throw;
            }
        }).Compile();

        await graph.RunAsync("http", new Job(), new InMemoryCheckpointStore());

        Assert.Equal(NodeFailureKind.Transient, failure!.Kind);
        Assert.Equal("tool 'flaky' failed: unavailable", failure.Message);
    }

    [Fact]
    public async Task An_Agents_Turns_Carry_Its_Id_And_Its_Tool_Calls_Are_Spans_Of_The_Attempt()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var chat = new ScriptedChatClient(Scripted.Tools(("1", "nope", "{}")), Scripted.Text("ok"));
        var agent = new StrataAgent<string>(chat, new ToolRegistry(), Options<string>("span-agent"));

        await AgentNode(agent).RunAsync("agent-span-thread", new Job(), new InMemoryCheckpointStore());

        var ofThread = spans.Where(s => (s.GetTagItem("strata.thread_id") as string) == "agent-span-thread").ToList();
        var attempt = ofThread.Single(s => s.OperationName == "strata.attempt");
        var turns = ofThread.Where(s => s.OperationName == "strata.model").ToList();
        Assert.Equal(2, turns.Count);
        Assert.All(turns, t =>
        {
            Assert.Equal("span-agent", t.GetTagItem("strata.agent"));
            Assert.Equal(attempt.SpanId, t.ParentSpanId);
        });
        Assert.Equal(attempt.SpanId, ofThread.Single(s => s.OperationName == "strata.tool").ParentSpanId);
    }
}

/// <summary>Tools for <see cref="AgentTests"/>.</summary>
public partial class AgentTools
{
    [StrataTool(Name = "k8s.pods", Description = "Lists a namespace's pods.")]
    public static string Pods(string ns) => $"3 pods in {ns}";
}
