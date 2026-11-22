// Samples for https://stratamesh.dev/guides/ai/model-routes/

// docs:begin guides/ai/model-routes/build-route
using Datalium.Strata.AI;
// docs:hide
using System.Collections.Concurrent;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Tools;
using Microsoft.Extensions.Time.Testing;
// docs:show
using Microsoft.Extensions.AI;

// docs:hide
namespace Datalium.Strata.DocsSnippets.Guides.AI.ModelRoutes;

[Collection(ModelRoutesConsole.Name)]
public class ModelRoutesSamples
{
    private const string RollBack = "Roll back deploy 412, because the error rate rose with it.";

    [Fact]
    public async Task FailOverAndSeeWhichCandidateAnswered()
    {
        using var console = new ConsoleOutput();

        // Scripted to fail with a 429, refuse, give an answer without a reason, and answer well.
        var northwind = new FakeModel("northwind", "nw-large", Turn.Fail(429, TimeSpan.FromSeconds(30)));
        var contoso = new FakeModel("contoso", "ct-guarded", Turn.Refuse("I can't help with production systems."));
        var fabrikam = new FakeModel("fabrikam", "fb-mini", Turn.Answer("Restart it."));
        var local = new FakeModel("local", "llama-local", Turn.Answer(RollBack));

        // docs:show
        var triage = new ModelRoute(new ModelRouteOptions
        {
            Name = "triage",
            Candidates =
            [
                new ModelCandidate("primary", northwind, model: "nw-large"),
                new ModelCandidate("careful", contoso, model: "ct-guarded"),
                new ModelCandidate("terse", fabrikam, model: "fb-mini"),
                new ModelCandidate("local", local, model: "llama-local"),
            ],
            // Your rule for a usable answer: here, it must give a reason.
            Accept = response => response.Text.Contains("because", StringComparison.OrdinalIgnoreCase) ? null : "it gives no reason",
        });
        // docs:end guides/ai/model-routes/build-route

        // An agent's route and tools, for the agent in the next sample.
        var analyst = new FakeModel("local", "llama-local", Turn.Answer("""{"cause":"deploy 412","action":"roll it back"}"""));
        var route = new ModelRoute(new ModelRouteOptions { Name = "rca", Candidates = [new ModelCandidate("local", analyst)] });
        var tools = new ToolRegistry();

        // docs:begin guides/ai/model-routes/use-route
        var graph = new StateGraph<Ticket>()
            .AddModelNode<Ticket>("Ask", triage,
                (ticket, _) => [new(ChatRole.User, ticket.Question)],
                (ticket, response) => ticket with { Answer = response.Text })
            .Compile();

        // An agent takes a route the same way.
        var agent = new StrataAgent<RootCause>(route, tools, new StrataAgentOptions<RootCause> { Id = "rca-analyst" });
        // docs:end guides/ai/model-routes/use-route

        // The agent's turn goes through its route.
        var analysis = await new StateGraph<Ticket>()
            .AddAgentNode("Analyse", agent, (t, _) => [new(ChatRole.User, t.Question)], (t, r) => t with { Answer = r.Output.Action })
            .Compile()
            .RunAsync("model-routes-rca", new Ticket("Why is checkout failing?"), new InMemoryCheckpointStore());
        Assert.Equal("roll it back", Assert.IsType<GraphExecutionResult<Ticket>.Completed>(analysis).State.Answer);

        var thread = "model-routes-triage";
        var ticket = new Ticket("Why is checkout failing?");
        var store = new InMemoryCheckpointStore();

        // docs:begin guides/ai/model-routes/see-routed
        var events = new RunEventChannel();
        var result = await graph.RunAsync(thread, ticket, store, new RunOptions { Observer = events });

        await foreach (var runEvent in events.Reader.ReadAllAsync())
        {
            if (runEvent is ModelRouted routed)
            {
                Console.WriteLine($"route '{routed.Route}': answered by {routed.Answered ?? "nobody"} ({routed.Model}), hedges {routed.Hedges}");
                foreach (var attempt in routed.Attempts)
                {
                    Console.WriteLine($"  {attempt.Candidate,-9} {attempt.Outcome,-12} {attempt.Reason}".TrimEnd());
                }
            }
        }
        // docs:end guides/ai/model-routes/see-routed

        // What the page says the run prints.
        Assert.Equal(
            """
            route 'triage': answered by local (llama-local), hedges 0
              primary   Failed       429
              careful   Refused
              terse     Rejected     it gives no reason
              local     Answered

            """,
            console.Text);
        Assert.Equal(RollBack, Assert.IsType<GraphExecutionResult<Ticket>.Completed>(result).State.Answer);

        // Each candidate was asked once, for the model named on it.
        Assert.Equal(["nw-large", "ct-guarded", "fb-mini", "llama-local"], new[] { northwind, contoso, fabrikam, local }.Select(m => m.AskedFor.Single()));
    }

    [Fact]
    public async Task HedgeASlowCandidate()
    {
        // primary hangs until it is cancelled.
        var northwind = new FakeModel("northwind", "nw-large", Turn.Hang());
        var local = new FakeModel("local", "llama-local", Turn.Answer(RollBack));

        // docs:begin guides/ai/model-routes/hedge
        var hedged = new ModelRoute(new ModelRouteOptions
        {
            Name = "hedged",
            Candidates =
            [
                new ModelCandidate("primary", northwind, model: "nw-large"),
                new ModelCandidate("local", local, model: "llama-local"),
            ],
            HedgeAfter = TimeSpan.FromSeconds(2),
        });
        // docs:end guides/ai/model-routes/hedge

        var (result, events) = await Run(AskGraph(hedged), "model-routes-hedged", new Ticket("Why is checkout failing?"));

        Assert.Equal(RollBack, Assert.IsType<GraphExecutionResult<Ticket>.Completed>(result).State.Answer);
        var routed = events.OfType<ModelRouted>().Single();
        Assert.Equal("primary:LostRace, local:Answered", Outcomes(routed));
        Assert.Equal(1, routed.Hedges);
        Assert.Equal(("local", "llama-local"), (routed.Answered, routed.Model));
    }

    [Fact]
    public async Task ChooseCandidatesPerRequest()
    {
        var northwind = new FakeModel("northwind", "nw-large", Turn.Answer(RollBack));
        var local = new FakeModel("local", "llama-local", Turn.Answer(RollBack));

        // docs:begin guides/ai/model-routes/eligible
        var plans = new ModelRoute(new ModelRouteOptions
        {
            Name = "plans",
            Candidates =
            [
                new ModelCandidate("frontier", northwind, model: "nw-large")
                {
                    // A tenant rule: the run's metadata says the customer's plan.
                    Eligible = (_, _, ctx) => ctx?.Metadata.GetValueOrDefault("plan") == "free"
                        ? "the free plan doesn't use frontier models"
                        : null,
                },
                new ModelCandidate("local", local, model: "llama-local")
                {
                    // A context-size rule: the local model takes short prompts only.
                    Eligible = (messages, _, _) => messages.Sum(m => m.Text.Length) > 4_000
                        ? "the prompt is too long for its context"
                        : null,
                },
            ],
        });
        // docs:end guides/ai/model-routes/eligible

        var graph = AskGraph(plans);
        var question = new Ticket("Why is checkout failing?");

        // The three runs on the page, with plan set in the run's metadata.
        var (paid, paidEvents) = await Run(graph, "model-routes-plan-paid", question, new() { ["plan"] = "paid" });
        Assert.IsType<GraphExecutionResult<Ticket>.Completed>(paid);
        Assert.Equal("frontier:Answered, local:NotTried", Outcomes(paidEvents.OfType<ModelRouted>().Single()));

        var (free, freeEvents) = await Run(graph, "model-routes-plan-free", question, new() { ["plan"] = "free" });
        Assert.IsType<GraphExecutionResult<Ticket>.Completed>(free);
        Assert.Equal(
            "frontier:Ineligible:the free plan doesn't use frontier models, local:Answered",
            Outcomes(freeEvents.OfType<ModelRouted>().Single()));

        var (long_, longEvents) = await Run(
            graph, "model-routes-plan-free-long", new Ticket(new string('?', 4_001)), new() { ["plan"] = "free" });
        var failed = Assert.IsType<GraphExecutionResult<Ticket>.NodeFailed>(long_);
        Assert.Equal(
            "frontier:Ineligible:the free plan doesn't use frontier models, local:Ineligible:the prompt is too long for its context",
            Outcomes(longEvents.OfType<ModelRouted>().Single()));
        Assert.Equal(("Ask", 1), (failed.Node, failed.Attempts));
        Assert.Equal(
            "model call failed (route_ineligible): no candidate of route 'plans' can take this request: frontier is ineligible "
            + "(the free plan doesn't use frontier models), local is ineligible (the prompt is too long for its context)",
            failed.Message);
        Assert.Equal((1, 1), (northwind.Calls, local.Calls));
    }

    [Fact]
    public async Task ShareHealthBetweenRoutes()
    {
        var clock = new FakeTimeProvider();

        // Two models of one provider, northwind, and a steady local model.
        var large = new FakeModel("northwind", "nw-large", Turn.Fail(503), Turn.Answer(RollBack));
        var small = new FakeModel("northwind", "nw-small", Turn.Fail(503), Turn.Answer("Deploy 412 broke checkout."));
        var steady = new FakeModel("local", "llama-local",
            Turn.Answer(RollBack), Turn.Answer("Deploy 412 broke checkout."),
            Turn.Answer(RollBack), Turn.Answer("Deploy 412 broke checkout."));

        // docs:begin guides/ai/model-routes/health
        var health = new ModelHealth(new ModelHealthOptions
        {
            Breaker = new ModelBreaker(Failures: 2, OpenFor: TimeSpan.FromSeconds(60)),
            // docs:hide
            TimeProvider = clock,
            // docs:show
        });

        var diagnose = new ModelRoute(new ModelRouteOptions
        {
            Name = "diagnose",
            Candidates = [new ModelCandidate("large", large), new ModelCandidate("steady", steady)],
            Health = health,
        });
        var summarise = new ModelRoute(new ModelRouteOptions
        {
            Name = "summarise",
            Candidates = [new ModelCandidate("small", small), new ModelCandidate("steady", steady)],
            Health = health,
        });
        // docs:end guides/ai/model-routes/health

        // A graph runs diagnose, then summarise.
        var graph = new StateGraph<Ticket>()
            .AddModelNode<Ticket>("Diagnose", diagnose, (t, _) => [new(ChatRole.User, t.Question)], (t, r) => t with { Answer = r.Text })
            .AddModelNode<Ticket>("Summarise", summarise, (t, _) => [new(ChatRole.User, t.Answer)], (t, r) => t with { Summary = r.Text })
            .AddEdge("Diagnose", "Summarise")
            .Compile();
        var question = new Ticket("Why is checkout failing?");

        // Run 1: northwind fails once in each route, two failures in a row on one key, so the breaker opens.
        var (_, events) = await Run(graph, "model-routes-health-1", question);
        Assert.Equal(["large:Failed:503, steady:Answered", "small:Failed:503, steady:Answered"], events.OfType<ModelRouted>().Select(Outcomes));
        Assert.Equal("local=Available(0), northwind=Open(2)", Snapshot(health));

        // Run 2 skips northwind in both routes without calling it.
        var (result, events2) = await Run(graph, "model-routes-health-2", question);
        Assert.Equal(["large:BreakerOpen:60 s, steady:Answered", "small:BreakerOpen:60 s, steady:Answered"], events2.OfType<ModelRouted>().Select(Outcomes));
        var answered = Assert.IsType<GraphExecutionResult<Ticket>.Completed>(result).State;
        Assert.Equal((RollBack, "Deploy 412 broke checkout."), (answered.Answer, answered.Summary));
        Assert.Equal((1, 1), (large.Calls, small.Calls));

        // After 60 seconds the key is half-open, and run 3's test call succeeds and closes it.
        clock.Advance(TimeSpan.FromSeconds(60));
        Assert.Equal("local=Available(0), northwind=HalfOpen(2)", Snapshot(health));
        var (_, events3) = await Run(graph, "model-routes-health-3", question);
        Assert.Equal(["large:Answered, steady:NotTried", "small:Answered, steady:NotTried"], events3.OfType<ModelRouted>().Select(Outcomes));
        Assert.Equal("local=Available(0), northwind=Available(0)", Snapshot(health));
    }

    [Fact]
    public async Task CallARouteDirectly()
    {
        using var console = new ConsoleOutput();

        // docs:begin guides/ai/model-routes/test-route
        var primary = new FakeModel("northwind", "nw-large", Turn.Fail(503));
        var backup = new FakeModel("local", "llama-local", Turn.Answer(RollBack));
        ModelRouteResult? routed = null;
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "triage",
            Candidates = [new ModelCandidate("primary", primary), new ModelCandidate("backup", backup)],
            OnRouted = result => routed = result,
        });

        var response = await route.GetResponseAsync([new(ChatRole.User, "Why is checkout failing?")]);

        Console.WriteLine(response.Text);
        Console.WriteLine(string.Join(", ", routed!.Attempts.Select(a => $"{a.Candidate}={a.Outcome}")));
        // docs:end guides/ai/model-routes/test-route

        Assert.Equal($"{RollBack}\nprimary=Failed, backup=Answered\n", console.Text);
    }

    private static CompiledGraph<Ticket> AskGraph(IChatClient chat) =>
        new StateGraph<Ticket>()
            .AddModelNode<Ticket>("Ask", chat, (ticket, _) => [new(ChatRole.User, ticket.Question)], (ticket, response) => ticket with { Answer = response.Text })
            .Compile();

    private static async Task<(GraphExecutionResult<Ticket> Result, List<RunEvent> Events)> Run(
        CompiledGraph<Ticket> graph, string thread, Ticket ticket, Dictionary<string, string>? metadata = null)
    {
        var channel = new RunEventChannel();
        var result = await graph.RunAsync(
            thread, ticket, new InMemoryCheckpointStore(), new RunOptions { Observer = channel, Metadata = metadata ?? [] });
        using var patience = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync(patience.Token))
        {
            events.Add(e);
        }

        return (result, events);
    }

    /// <summary><c>candidate:outcome[:reason]</c> for each candidate, in the route's order.</summary>
    private static string Outcomes(ModelRouted routed) =>
        string.Join(", ", routed.Attempts.Select(a => a.Reason is null ? $"{a.Candidate}:{a.Outcome}" : $"{a.Candidate}:{a.Outcome}:{a.Reason}"));

    private static string Snapshot(ModelHealth health) =>
        string.Join(", ", health.Snapshot().OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => $"{e.Key}={e.State}({e.Failures})"));
}

internal sealed record Ticket(string Question, string Answer = "", string Summary = "");

internal sealed record RootCause(string Cause, string Action);

/// <summary>One scripted turn of a <see cref="FakeModel"/>.</summary>
internal sealed record Turn
{
    public string Text { get; init; } = "";

    /// <summary>Answer with finish reason content_filter.</summary>
    public bool Refused { get; init; }

    public Exception? Failure { get; init; }

    /// <summary>Wait until the call is cancelled (a hedge's loser).</summary>
    public bool Hanging { get; init; }

    public static Turn Answer(string text) => new() { Text = text };

    public static Turn Refuse(string text) => new() { Text = text, Refused = true };

    public static Turn Hang() => new() { Hanging = true };

    // docs:begin guides/ai/model-routes/fake-fault
    public static Turn Fail(int status, TimeSpan? retryAfter = null, string? message = null) => new()
    {
        Failure = new ModelCallException(
            message ?? $"{status} from the provider",
            isTransient: ModelFaults.IsTransientStatus(status),
            statusCode: status,
            retryAfter: retryAfter),
    };

    // docs:hide In FakeModel: the provider name is the default health key.
}

/// <summary>
/// A scripted stand-in for a provider's chat client: it plays its turns in order and names its provider and model in
/// its metadata.
/// </summary>
internal sealed class FakeModel(string provider, string model, params Turn[] script) : IChatClient
{
    private readonly ConcurrentQueue<Turn> _script = new(script);
    private int _calls;

    public int Calls => Volatile.Read(ref _calls);

    /// <summary>The model each call asked for.</summary>
    public ConcurrentQueue<string?> AskedFor { get; } = new();

    public async Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _calls);
        AskedFor.Enqueue(options?.ModelId);
        if (!_script.TryDequeue(out var turn))
        {
            throw new InvalidOperationException($"{model} has no more scripted turns");
        }

        if (turn.Hanging)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        await Task.Yield();
        if (turn.Failure is { } failure)
        {
            throw failure;
        }

        return new ChatResponse(new ChatMessage(ChatRole.Assistant, turn.Text))
        {
            ModelId = options?.ModelId ?? model,
            FinishReason = turn.Refused ? ChatFinishReason.ContentFilter : ChatFinishReason.Stop,
            Usage = new UsageDetails { InputTokenCount = 20, OutputTokenCount = 8 },
        };
    }

    public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("these runs don't stream");

    // docs:show
    public object? GetService(Type serviceType, object? serviceKey = null) =>
        serviceType == typeof(ChatClientMetadata) ? new ChatClientMetadata(provider, null, model) : null;
    // docs:end guides/ai/model-routes/fake-fault

    public void Dispose()
    {
    }
}

/// <summary>The tests that check what a sample prints run alone, so nothing else writes to the console meanwhile.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ModelRoutesConsole
{
    public const string Name = "guides/ai/model-routes console";
}

/// <summary>Captures what a sample writes to the console, with \n line endings.</summary>
internal sealed class ConsoleOutput : IDisposable
{
    private readonly TextWriter _original = Console.Out;
    private readonly StringWriter _writer = new();

    public ConsoleOutput() => Console.SetOut(_writer);

    public string Text => _writer.ToString().ReplaceLineEndings("\n");

    public void Dispose() => Console.SetOut(_original);
}
