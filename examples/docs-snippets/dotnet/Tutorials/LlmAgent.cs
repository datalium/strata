// Samples for https://stratamesh.dev/tutorials/llm-agent/. The tutorial builds one program: its Program.cs is the
// tutorials/llm-agent/program region below (the FullProgram test, with --scripted), and its PicnicTools.cs and
// ScriptedModel.cs are in LlmAgent/. The steps that show code in another order than Program.cs are in
// LlmAgentStepsSamples, with their own copies of the types.

// docs:begin tutorials/llm-agent/program
using System.ClientModel;
using System.ClientModel.Primitives;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;
using OpenAI;

// docs:hide
namespace Datalium.Strata.DocsSnippets.Tutorials.LlmAgent;

[Collection(ConsoleOutput.Collection)]
public partial class LlmAgentSamples
{
    [Fact]
    public async Task FullProgram()
    {
        string[] args = ["--scripted"];
        using var console = ConsoleOutput.Capture();

        // docs:show
        // 1. The chat client: a local model, or a scripted stand-in with --scripted.
        // docs:begin tutorials/llm-agent/choose-client
        using IChatClient chat = args.Contains("--scripted")
            ? Script()
            : CreateClient("qwen2.5:3b", "http://localhost:11434/v1", apiKey: null);
        // docs:end tutorials/llm-agent/choose-client

        // 2. The tools the agent may call.
        // docs:begin tutorials/llm-agent/register
        var tools = new ToolRegistry().Register(new PicnicTools());
        // docs:end tutorials/llm-agent/register

        // 3. The agent, with a typed answer.
        // docs:begin tutorials/llm-agent/agent
        var agent = new StrataAgent<PicnicPlan>(chat, tools, new StrataAgentOptions<PicnicPlan>
        {
            Id = "picnic-planner",
            Chat = new ChatOptions { Temperature = 0 },
            MaxTurns = 4,
            Validate = Validate,
            // Ollama holds the model to the response format on every turn that asks for one, so the
            // model can't call a tool there. Ask for the JSON schema only on a repair turn.
            ConfigureTurn = (turn, options) =>
            {
                if (!turn.IsRepair)
                {
                    options.ResponseFormat = null;
                }
            },
        });
        // docs:end tutorials/llm-agent/agent

        // 4. The graph: the agent plans, then a plain node writes the message.
        var graph = new StateGraph<PicnicRequest>()
            .AddAgentNode("Plan", agent,
                prompt: (request, _) =>
                [
                    new(ChatRole.System,
                        "You plan picnics. Always call weather.forecast and parks.search before you answer. " +
                        "Go ahead only if the forecast is dry. Then answer with only a JSON object with the fields " +
                        "city, day, goAhead (true or false), park (one park from parks.search, or null) and reason."),
                    new(ChatRole.User, $"Can I have a picnic in {request.City} on {request.Day}?"),
                ],
                apply: (request, result) => request with
                {
                    Plan = result.Output,
                    Turns = result.Turns,
                    ToolsUsed = [.. result.ToolCalls.Select(call => call.Name)],
                })
            .AddNode("Announce", request => request with
            {
                Message = request.Plan!.GoAhead
                    ? $"Picnic on: {request.Plan.Park}, {request.Day}. {request.Plan.Reason}"
                    : $"No picnic {request.Day}. {request.Plan.Reason}",
            })
            .AddEdge("Plan", "Announce")
            .SetRecoveryPolicy("Plan", RecoveryPolicy.Retry(1))
            .Compile();

        // 5. Run it, streaming the model's text as it arrives.
        // docs:begin tutorials/llm-agent/stream
        var (chunks, completion) = graph.RunStreaming(
            "picnic-1", new PicnicRequest("Lisbon", "Saturday"), new InMemoryCheckpointStore());

        await foreach (var chunk in chunks.ReadAllAsync())
        {
            switch (chunk.Type)
            {
                case StreamChunkType.Token:
                    Console.Write(chunk.Payload);
                    break;
                case StreamChunkType.Retry:
                    Console.WriteLine($"\n[retry] {chunk.NodeName}");
                    break;
            }
        }

        var result = await completion;
        if (result is GraphExecutionResult<PicnicRequest>.Completed { State: var done })
        {
            Console.WriteLine();
            Console.WriteLine($"tools used: {string.Join(", ", done.ToolsUsed)}");
            Console.WriteLine($"model turns: {done.Turns}");
            Console.WriteLine(done.Message);
        }
        else
        {
            Console.WriteLine($"\nThe run did not complete: {result}");
        }
        // docs:end tutorials/llm-agent/stream

        // docs:hide
        var completed = Assert.IsType<GraphExecutionResult<PicnicRequest>.Completed>(result);
        Assert.Equal(new[] { "weather.forecast", "parks.search" }, completed.State.ToolsUsed);
        Assert.Equal(ScriptedOutput, console.Text);
    }

    // docs:show
    // docs:begin tutorials/llm-agent/client-helper
    static IChatClient CreateClient(string model, string? baseUrl, string? apiKey) =>
        new OpenAI.Chat.ChatClient(
            model,
            new ApiKeyCredential(apiKey ?? "not-needed"),
            new OpenAIClientOptions
            {
                RetryPolicy = new ClientRetryPolicy(maxRetries: 0),
                Endpoint = baseUrl is null ? null : new Uri(baseUrl),
            })
        .AsIChatClient();
    // docs:end tutorials/llm-agent/client-helper

    static IReadOnlyList<string> Validate(PicnicPlan plan) =>
        plan.GoAhead && string.IsNullOrWhiteSpace(plan.Park) ? ["a picnic that goes ahead needs a park"] : [];

    // docs:begin tutorials/llm-agent/script
    static ScriptedModel Script() => new(
        ScriptedModel.Call("call_1", "weather_forecast", new() { ["city"] = "Lisbon", ["day"] = "Saturday" }),
        ScriptedModel.Call("call_2", "parks_search", new() { ["city"] = "Lisbon" }),
        ScriptedModel.Say("""{"city":"Lisbon","day":"Saturday","goAhead":true,"park":"Jardim da Estrela","reason":"Sunny and 24°C, with a 5% chance of rain."}"""));
    // docs:end tutorials/llm-agent/script

    // docs:hide
}

// docs:show
public sealed record PicnicPlan(string City, string Day, bool GoAhead, string? Park, string Reason);

public sealed record PicnicRequest(string City, string Day)
{
    public PicnicPlan? Plan { get; init; }
    public int Turns { get; init; }
    public IReadOnlyList<string> ToolsUsed { get; init; } = [];
    public string? Message { get; init; }
}
// docs:end tutorials/llm-agent/program

public partial class LlmAgentSamples
{
    /// <summary>What the scripted run prints, on every run (the page shows it after step 8).</summary>
    internal const string ScriptedOutput = """
        [tool] weather.forecast(Lisbon, Saturday)
        [tool] parks.search(Lisbon)
        {"city":"Lisbon","day":"Saturday","goAhead":true,"park":"Jardim da Estrela","reason":"Sunny and 24°C, with a 5% chance of rain."}
        tools used: weather.forecast, parks.search
        model turns: 3
        Picnic on: Jardim da Estrela, Saturday. Sunny and 24°C, with a 5% chance of rain.

        """;

    [Fact]
    public void CreateTheClient()
    {
        // docs:begin tutorials/llm-agent/create-client
        using IChatClient chat = CreateClient("qwen2.5:3b", "http://localhost:11434/v1", apiKey: null);
        // docs:end tutorials/llm-agent/create-client

        var metadata = chat.GetService<ChatClientMetadata>();
        Assert.Equal("qwen2.5:3b", metadata?.DefaultModelId);
        Assert.Equal(new Uri("http://localhost:11434/v1"), metadata?.ProviderUri);
    }

    [Fact]
    public void UseAHostedModel()
    {
        // docs:begin tutorials/llm-agent/hosted
        using IChatClient chat = CreateClient(
            "gpt-4o-mini", baseUrl: null, apiKey: Environment.GetEnvironmentVariable("OPENAI_API_KEY"));
        // docs:end tutorials/llm-agent/hosted

        // Building the client calls nothing, so it needs no key until it is used.
        var metadata = chat.GetService<ChatClientMetadata>();
        Assert.Equal("gpt-4o-mini", metadata?.DefaultModelId);
        Assert.Equal("api.openai.com", metadata?.ProviderUri?.Host);
    }
}

/// <summary>
/// The steps whose code comes in another order than in Program.cs: the answer type before its validate function,
/// and the graph before the state's record. They have their own copies of the types, nested here.
/// </summary>
[Collection(ConsoleOutput.Collection)]
public class LlmAgentStepsSamples
{
    // docs:begin tutorials/llm-agent/answer-type
    public sealed record PicnicPlan(string City, string Day, bool GoAhead, string? Park, string Reason);

    static IReadOnlyList<string> Validate(PicnicPlan plan) =>
        plan.GoAhead && string.IsNullOrWhiteSpace(plan.Park) ? ["a picnic that goes ahead needs a park"] : [];
    // docs:end tutorials/llm-agent/answer-type

    [Fact]
    public void AnswerType()
    {
        var plan = new PicnicPlan("Lisbon", "Saturday", GoAhead: true, Park: null, "Sunny.");
        Assert.Equal(new[] { "a picnic that goes ahead needs a park" }, Validate(plan));
        Assert.Empty(Validate(plan with { Park = "Jardim da Estrela" }));
        Assert.Empty(Validate(plan with { GoAhead = false }));
    }

    [Fact]
    public async Task RunIt()
    {
        using var console = ConsoleOutput.Capture();
        using IChatClient chat = new ScriptedModel(
            ScriptedModel.Call("call_1", "weather_forecast", new() { ["city"] = "Lisbon", ["day"] = "Saturday" }),
            ScriptedModel.Call("call_2", "parks_search", new() { ["city"] = "Lisbon" }),
            ScriptedModel.Say("""{"city":"Lisbon","day":"Saturday","goAhead":true,"park":"Jardim da Estrela","reason":"Sunny and 24°C, with a 5% chance of rain."}"""));
        var agent = new StrataAgent<PicnicPlan>(chat, new ToolRegistry().Register(new PicnicTools()), new StrataAgentOptions<PicnicPlan>
        {
            Id = "picnic-planner",
            Chat = new ChatOptions { Temperature = 0 },
            MaxTurns = 4,
            Validate = Validate,
            ConfigureTurn = (turn, options) =>
            {
                if (!turn.IsRepair)
                {
                    options.ResponseFormat = null;
                }
            },
        });
        var graph = BuildGraph(agent);

        // docs:begin tutorials/llm-agent/run
        var result = await graph.RunAsync(
            "picnic-1", new PicnicRequest("Lisbon", "Saturday"), new InMemoryCheckpointStore());

        if (result is GraphExecutionResult<PicnicRequest>.Completed { State: var done })
        {
            Console.WriteLine($"tools used: {string.Join(", ", done.ToolsUsed)}");
            Console.WriteLine($"model turns: {done.Turns}");
            Console.WriteLine(done.Message);
        }
        else
        {
            Console.WriteLine($"The run did not complete: {result}");
        }
        // docs:end tutorials/llm-agent/run

        Assert.IsType<GraphExecutionResult<PicnicRequest>.Completed>(result);
        Assert.Equal(
            """
            [tool] weather.forecast(Lisbon, Saturday)
            [tool] parks.search(Lisbon)
            tools used: weather.forecast, parks.search
            model turns: 3
            Picnic on: Jardim da Estrela, Saturday. Sunny and 24°C, with a 5% chance of rain.

            """,
            console.Text);
    }

    private static CompiledGraph<PicnicRequest> BuildGraph(StrataAgent<PicnicPlan> agent)
    {
        // docs:begin tutorials/llm-agent/graph
        var graph = new StateGraph<PicnicRequest>()
            .AddAgentNode("Plan", agent,
                prompt: (request, _) =>
                [
                    new(ChatRole.System,
                        "You plan picnics. Always call weather.forecast and parks.search before you answer. " +
                        "Go ahead only if the forecast is dry. Then answer with only a JSON object with the fields " +
                        "city, day, goAhead (true or false), park (one park from parks.search, or null) and reason."),
                    new(ChatRole.User, $"Can I have a picnic in {request.City} on {request.Day}?"),
                ],
                apply: (request, result) => request with
                {
                    Plan = result.Output,
                    Turns = result.Turns,
                    ToolsUsed = [.. result.ToolCalls.Select(call => call.Name)],
                })
            .AddNode("Announce", request => request with
            {
                Message = request.Plan!.GoAhead
                    ? $"Picnic on: {request.Plan.Park}, {request.Day}. {request.Plan.Reason}"
                    : $"No picnic {request.Day}. {request.Plan.Reason}",
            })
            .AddEdge("Plan", "Announce")
            .SetRecoveryPolicy("Plan", RecoveryPolicy.Retry(1))
            .Compile();

        // docs:hide
        return graph;
    }

    // docs:show
    public sealed record PicnicRequest(string City, string Day)
    {
        public PicnicPlan? Plan { get; init; }
        public int Turns { get; init; }
        public IReadOnlyList<string> ToolsUsed { get; init; } = [];
        public string? Message { get; init; }
    }
    // docs:end tutorials/llm-agent/graph
}

/// <summary>
/// What the samples print, read back by their tests. Console.Out is the process's, so the tests that read it are in
/// one collection that runs alone.
/// </summary>
internal sealed class ConsoleOutput : IDisposable
{
    public const string Collection = "tutorials/llm-agent console";

    private readonly TextWriter _original = Console.Out;
    private readonly StringWriter _captured = new();

    private ConsoleOutput() => Console.SetOut(_captured);

    public static ConsoleOutput Capture() => new();

    public string Text => _captured.ToString().ReplaceLineEndings("\n");

    public void Dispose()
    {
        Console.SetOut(_original);
        _captured.Dispose();
    }
}

[CollectionDefinition(ConsoleOutput.Collection, DisableParallelization = true)]
public sealed class ConsoleOutputCollection
{
}
