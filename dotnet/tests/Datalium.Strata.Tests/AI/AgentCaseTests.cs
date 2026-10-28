using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Tests.Scheduling;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// The agent's shared cases (<c>design/44-LLM-Integration.md</c>, "Tests"): <c>schema/fixtures/ai/agent-cases.json</c>,
/// which <c>rust/strata-ai/tests/agent_cases.rs</c> replays too. Keep the two in step.
/// </summary>
public sealed class AgentCaseTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Diagnosis(string Cause, double Confidence, IReadOnlyList<string> Evidence);

    public sealed record Job(bool Done = false);

    private static JsonArray Cases() => RunSchedulerTests.Fixture(Path.Combine("ai", "agent-cases.json"))["cases"]!.AsArray();

    private static JsonNode Case(string name) => Cases().Single(c => c!["name"]!.GetValue<string>() == name)!;

    public static TheoryData<string> CaseNames()
    {
        var names = new TheoryData<string>();
        foreach (var @case in Cases())
        {
            names.Add(@case!["name"]!.GetValue<string>());
        }

        return names;
    }

    /// <summary>A tool as a case describes it.</summary>
    private sealed class CaseTool(string name, JsonNode spec) : IStrataToolProvider
    {
        public string GetToolsManifest() =>
            new JsonArray(new JsonObject
            {
                ["name"] = name,
                ["description"] = $"The case's {name} tool.",
                ["parameters"] = new JsonObject { ["type"] = "object", ["properties"] = new JsonObject(), ["required"] = new JsonArray() },
            }).ToJsonString();

        public Task<string> DispatchToolAsync(string toolName, string jsonArguments) =>
            DispatchToolAsync(toolName, jsonArguments, ToolCallContext.None);

        public async Task<string> DispatchToolAsync(
            string toolName, string jsonArguments, ToolCallContext context, CancellationToken cancellationToken = default)
        {
            if (spec["delay_ms"]?.GetValue<int>() is { } delay)
            {
                await Task.Delay(delay, cancellationToken);
            }

            if (spec["expects"]?.GetValue<string>() is { } field
                && JsonNode.Parse(jsonArguments)?[field]?.GetValueKind() != JsonValueKind.String)
            {
                // What the generated dispatch throws for arguments it can't read.
                throw new ToolArgumentsException(toolName, $"argument '{field}' must be a string");
            }

            if (spec["fails"] is { } fails)
            {
                var message = fails["message"]!.GetValue<string>();
                throw fails["transient"]?.GetValue<bool>() == true
                    ? NodeException.Transient(message)
                    : new InvalidOperationException(message);
            }

            return spec["answers"]?.GetValue<string>() ?? string.Empty;
        }
    }

    /// <summary>Denies the calls to the tools a case marks <c>denied</c>.</summary>
    private sealed class Deny(Dictionary<string, string> reasons) : IToolMiddleware
    {
        public Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken) =>
            reasons.TryGetValue(call.Name, out var reason)
                ? throw new ToolCallDeniedException(call.Name, reason)
                : next(call, cancellationToken);
    }

    private static Scripted ScriptedTurn(JsonNode turn)
    {
        if (turn["text"] is { } text)
        {
            return Scripted.Text(text.GetValue<string>());
        }

        if (turn["tool_calls"] is JsonArray calls)
        {
            return Scripted.Tools([.. calls.Select(c => (
                c!["id"]!.GetValue<string>(),
                c["name"]!.GetValue<string>(),
                c["arguments"]!.GetValueKind() == JsonValueKind.String ? c["arguments"]!.GetValue<string>() : c["arguments"]!.ToJsonString()))]);
        }

        var fails = turn["fails"]!;
        var status = fails["status"]!.GetValue<int>();
        return Scripted.Fails(new ModelCallException(
            fails["message"]!.GetValue<string>(), ModelFaults.IsTransientStatus(status), status));
    }

    private static string? Describe(RunEvent e) => e switch
    {
        ModelCallStarted => "ModelCallStarted",
        ModelCallCompleted c => $"ModelCallCompleted {c.Result}",
        ToolCallStarted s => $"ToolCallStarted {s.Tool} {s.CallId} {s.CallerId}",
        ToolCallCompleted c => $"ToolCallCompleted {c.Tool} {c.CallId} {c.Result}",
        _ => null,
    };

    private sealed record Outcome(
        IReadOnlyList<string> Declarations, JsonNode? Output, AgentResult<object>? Result, NodeException? Failure, IReadOnlyList<string> Events);

    private static async Task<Outcome> Play<TOutput>(JsonNode @case, Func<TOutput, JsonNode?> output)
    {
        var options = @case["options"];
        var registry = new ToolRegistry();
        var denied = new Dictionary<string, string>(StringComparer.Ordinal);
        var subset = new List<string>();
        foreach (var tool in @case["tools"]!.AsArray())
        {
            var name = tool!["name"]!.GetValue<string>();
            registry.Register(new CaseTool(name, tool));
            if (tool["denied"]?.GetValue<string>() is { } reason)
            {
                denied[name] = reason;
            }

            if (tool["subset"]?.GetValue<bool>() != false)
            {
                subset.Add(name);
            }
        }

        registry.Use(new Deny(denied));
        var agentOptions = new StrataAgentOptions<TOutput>
        {
            Id = "investigator",
            MaxTurns = options?["max_turns"]?.GetValue<int>() ?? 8,
            MaxRepairs = options?["max_repairs"]?.GetValue<int>() ?? 1,
            ToolConcurrency = options?["tool_concurrency"]?.GetValue<int>() ?? 1,
            ToolFailures = options?["tool_failures"]?.GetValue<string>() == "return_to_model" ? ToolFailures.ReturnToModel : ToolFailures.FailNode,
            Validate = options?["validate"]?.GetValue<string>() == "confidence"
                ? o => output(o)!["confidence"]!.GetValue<double>() is >= 0 and <= 1 ? [] : ["confidence must be between 0 and 1"]
                : null,
        };
        var chat = new ScriptedChatClient([.. @case["model"]!.AsArray().Select(t => ScriptedTurn(t!))]);
        var agent = new StrataAgent<TOutput>(chat, registry.Subset(subset), agentOptions);

        AgentResult<TOutput>? produced = null;
        NodeException? failure = null;
        var graph = new StateGraph<Job>()
            .AddFallibleNode("Investigate", async (job, context, cancellationToken) =>
            {
                try
                {
                    produced = await agent.RunAsync(
                        [new(ChatRole.System, "Investigate."), new(ChatRole.User, "Why is checkout failing?")], context, cancellationToken);
                    return job with { Done = true };
                }
                catch (NodeException ex)
                {
                    failure = ex;
                    throw;
                }
            })
            .Compile();
        var channel = new RunEventChannel();
        var run = await graph.RunAsync("case", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });
        var events = new List<string>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            if (Describe(e) is { } described)
            {
                events.Add(described);
            }
        }

        if (failure is not null)
        {
            Assert.IsAssignableFrom<GraphExecutionResult<Job>.Failed>(run);
        }

        var declarations = agent.Declarations.Tools.Select(t => t.Name).ToList();
        if (produced is null)
        {
            return new Outcome(declarations, null, null, failure, events);
        }

        Assert.IsType<GraphExecutionResult<Job>.Completed>(run);
        var result = new AgentResult<object>(produced.Output!, produced.Messages, produced.Usage, produced.Turns, produced.ToolCalls);
        return new Outcome(declarations, output(produced.Output), result, failure, events);
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public async Task Each_Shared_Case_Plays_As_The_Fixture_Says(string name)
    {
        var @case = Case(name);
        var outcome = @case["options"]?["output"]?.GetValue<string>() == "diagnosis"
            ? await Play<Diagnosis>(@case, d => JsonSerializer.SerializeToNode(d, JsonSerializerOptions.Web))
            : await Play<string>(@case, text => JsonValue.Create(text));
        var expect = @case["expect"]!;

        if (expect["declarations"] is JsonArray declarations)
        {
            Assert.Equal(declarations.Select(d => d!.GetValue<string>()), outcome.Declarations);
        }

        if (expect["events"] is JsonArray events)
        {
            Assert.Equal(events.Select(e => e!.GetValue<string>()), outcome.Events);
        }

        if (expect["failure"] is { } expectedFailure)
        {
            Assert.NotNull(outcome.Failure);
            Assert.Equal(expectedFailure["transient"]!.GetValue<bool>(), outcome.Failure.Kind == NodeFailureKind.Transient);
            Assert.Equal(expectedFailure["message"]!.GetValue<string>(), outcome.Failure.Message);
            return;
        }

        Assert.Null(outcome.Failure);
        var result = outcome.Result!;
        Assert.True(JsonNode.DeepEquals(expect["output"], outcome.Output), $"output: {outcome.Output?.ToJsonString()}");
        Assert.Equal(expect["turns"]!.GetValue<int>(), result.Turns);
        Assert.Equal([expect["usage"]![0]!.GetValue<long>(), expect["usage"]![1]!.GetValue<long>()], [result.Usage.InputTokens, result.Usage.OutputTokens]);
        if (expect["tool_calls"] is JsonArray toolCalls)
        {
            Assert.Equal(
                toolCalls.Select(c => $"{c!["name"]} {c["call_id"]} {c["result"]}"),
                result.ToolCalls.Select(c => $"{c.Name} {c.CallId} {c.Result.ToString().ToLowerInvariant()}"));
        }

        var later = result.Messages.Skip(2).ToList();
        if (expect["tool_results"] is JsonArray toolResults)
        {
            var turns = later.Where(m => m.Role == ChatRole.Tool).ToList();
            Assert.Equal(toolResults.Count, turns.Count);
            foreach (var (expected, turn) in toolResults.Zip(turns))
            {
                var results = turn.Contents.OfType<FunctionResultContent>().ToList();
                Assert.Equal(expected!.AsArray().Count, results.Count);
                foreach (var (want, got) in expected.AsArray().Zip(results))
                {
                    Assert.Equal(want!["call_id"]!.GetValue<string>(), got.CallId);
                    var content = Assert.IsType<string>(got.Result);
                    if (want["content"] is { } exact)
                    {
                        Assert.Equal(exact.GetValue<string>(), content);
                    }
                    else
                    {
                        Assert.StartsWith(want["content_starts_with"]!.GetValue<string>(), content, StringComparison.Ordinal);
                    }
                }
            }
        }

        if (expect["repairs"] is JsonArray repairs)
        {
            var asked = later.Where(m => m.Role == ChatRole.User).Select(m => m.Text).ToList();
            Assert.Equal(repairs.Count, asked.Count);
            foreach (var (prefix, repair) in repairs.Zip(asked))
            {
                Assert.StartsWith(prefix!.GetValue<string>(), repair, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void Every_Case_Is_Played()
    {
        Assert.True(Cases().Count >= 20);
    }
}
