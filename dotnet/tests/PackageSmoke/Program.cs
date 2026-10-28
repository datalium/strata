using System.Runtime.CompilerServices;
using System.Text.Json;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Hosting;
using Datalium.Strata.Interruption;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

// The packed packages, used as an application would use them (design/36-Packaging-And-Versioning.md): one
// reference, to Datalium.Strata.Hosting. That this file compiles shows the engine, the stores and the hosting
// package arrived with it, and that the [StrataTool] generator inside Datalium.Strata reached this project
// through them: PricingTools has no dispatch code of its own. The agent and the model node come from
// Datalium.Strata.AI (design/44-LLM-Integration.md), answered by chat clients in this file: the agent's model asks
// for the generated tool under its provider-safe name and answers with what it returned, and the model node's call
// goes through a model route (design/46-Model-Routes.md) whose first candidate is down, so the second answers.
// Long-term memory (design/45-Long-Term-Memory.md) is registered by the hosting package over the engine's in-memory
// store and an embedding generator of this file's own, and the helper recalls a past order by its text.
var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
builder.Services.AddSingleton<IEmbeddingGenerator<string, Embedding<float>>>(new HashEmbeddingGenerator());
builder.Services.AddStrata()
    .AddInMemoryCheckpoints()
    .AddInMemoryMemory(o => o.Dimensions = 3)
    .AddMemory()
    .AddHealthChecks()
    .AddGraph(_ => new StateGraph<Order>()
        .AddAgentNode<Order, string>(
            "Price",
            new StrataAgent<string>(
                new PricingChatClient(),
                new ToolRegistry().Register(new PricingTools()),
                new StrataAgentOptions<string> { Id = "pricer" }),
            (order, _) => [new(ChatRole.User, $"Price {order.Quantity}.")],
            (order, result) => order with { Total = int.Parse(result.Output) })
        .AddModelNode<Order>(
            "Describe",
            new ModelRoute(new ModelRouteOptions
            {
                Name = "describe",
                Candidates = [new ModelCandidate("down", new DownChatClient()), new ModelCandidate("echo", new EchoChatClient())],
            }),
            (order, _) => [new(ChatRole.User, $"{order.Quantity} for {order.Total}")],
            (order, response) => order with { Note = response.Text })
        .AddEdge("Price", "Describe")
        .Compile());

using var host = builder.Build();
await host.StartAsync();

var result = await host.Services.GetRequiredService<StrataRunner<Order>>().RunAsync("smoke", new Order(3, 0, ""));
var health = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync();
var memory = host.Services.GetRequiredService<StrataMemory>();
await memory.RememberManyAsync(
[
    new MemoryEntry("orders", "small", "three for thirty"),
    new MemoryEntry("orders", "large", "three hundred for three thousand"),
]);
var recalled = await memory.RecallAsync("orders", "three for thirty", new RecallOptions { Limit = 1 });
await host.StopAsync();

if (result is not GraphExecutionResult<Order>.Completed { State: { Total: 30, Note: "echo: 3 for 30" } })
{
    Console.Error.WriteLine($"the graph did not price the order: {result}");
    return 1;
}

if (health.Status != HealthStatus.Healthy)
{
    Console.Error.WriteLine($"the host is not healthy: {health.Status}");
    return 1;
}

if (recalled is not [{ Record: { Id: "small", Model: "smoke/hash-3", Text: "three for thirty" }, Similarity: > 0.999 }])
{
    Console.Error.WriteLine($"the memory did not recall the order: {string.Join(", ", recalled)}");
    return 1;
}

if (!new PricingTools().GetToolsManifest().Contains("price_order", StringComparison.Ordinal))
{
    Console.Error.WriteLine("the generated manifest does not name the tool");
    return 1;
}

Console.WriteLine($"Datalium.Strata.Hosting {typeof(StrataRunner<>).Assembly.GetName().Version}: packed, restored, generated and run.");
return 0;

/// <summary>What the smoke graph prices.</summary>
public sealed record Order(int Quantity, int Total, string Note);

/// <summary>A tool whose dispatch code only exists if the source generator ran.</summary>
public partial class PricingTools
{
    [StrataTool(Name = "price_order", Description = "Prices an order of a quantity of the one product there is.")]
    public static Task<string> PriceOrderAsync(int quantity) => Task.FromResult((quantity * 10).ToString());
}

/// <summary>
/// A model that asks for <c>price_order</c> (as <c>price_order</c>: already provider-safe) with the quantity in the
/// first message, then answers with the price the tool gave.
/// </summary>
public sealed class PricingChatClient : IChatClient
{
    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        var said = messages.ToList();
        if (said.LastOrDefault()?.Contents.OfType<FunctionResultContent>().FirstOrDefault() is { } priced)
        {
            var total = JsonDocument.Parse((string)priced.Result!).RootElement.GetProperty("result").GetString()!;
            return Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, total)));
        }

        var tool = options?.Tools?.SingleOrDefault()?.Name ?? "missing";
        var quantity = int.Parse(said[0].Text.Split(' ')[1].TrimEnd('.'));
        return Task.FromResult(new ChatResponse(new ChatMessage(
            ChatRole.Assistant, [new FunctionCallContent("call-1", tool, new Dictionary<string, object?> { ["quantity"] = quantity })])));
    }

    public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var update in (await GetResponseAsync(messages, options, cancellationToken)).ToChatResponseUpdates())
        {
            yield return update;
        }
    }

    public object? GetService(Type serviceType, object? serviceKey = null) => null;

    public void Dispose()
    {
    }
}

/// <summary>A model that repeats the last message, for the model node.</summary>
public sealed class EchoChatClient : IChatClient
{
    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
        Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, $"echo: {messages.Last().Text}")));

    public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        yield return new ChatResponseUpdate(ChatRole.Assistant, $"echo: {messages.Last().Text}");
    }

    public object? GetService(Type serviceType, object? serviceKey = null) => null;

    public void Dispose()
    {
    }
}

/// <summary>
/// An embedding model of this file's own, <c>smoke/hash-3</c>: each character of a text counts in one of three
/// dimensions, so a text and itself are as similar as two texts can be.
/// </summary>
public sealed class HashEmbeddingGenerator : IEmbeddingGenerator<string, Embedding<float>>
{
    public Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values, EmbeddingGenerationOptions? options = null, CancellationToken cancellationToken = default)
    {
        var embeddings = new GeneratedEmbeddings<Embedding<float>>();
        foreach (var text in values)
        {
            var vector = new float[3];
            foreach (var c in text)
            {
                vector[c % 3]++;
            }

            embeddings.Add(new Embedding<float>(vector) { ModelId = "hash-3" });
        }

        return Task.FromResult(embeddings);
    }

    public object? GetService(Type serviceType, object? serviceKey = null) =>
        serviceType == typeof(EmbeddingGeneratorMetadata) ? new EmbeddingGeneratorMetadata("smoke", null, "hash-3") : null;

    public void Dispose()
    {
    }
}

/// <summary>A model whose provider can't be reached: a transient fault, which the route fails over from.</summary>
public sealed class DownChatClient : IChatClient
{
    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
        throw new HttpRequestException("connection refused");

    public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) =>
        throw new HttpRequestException("connection refused");

    public object? GetService(Type serviceType, object? serviceKey = null) => null;

    public void Dispose()
    {
    }
}
