using System.ClientModel;
using System.ClientModel.Primitives;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;
using OpenAI;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// Tests against a live OpenAI-compatible endpoint (<c>design/44-LLM-Integration.md</c>, "Tests"), through
/// <c>Microsoft.Extensions.AI.OpenAI</c>'s <see cref="IChatClient"/>: <c>STRATA_TEST_OPENAI_MODEL</c>, and
/// <c>STRATA_TEST_OPENAI_KEY</c> (OpenAI's API) or <c>STRATA_TEST_OPENAI_URL</c> (any compatible server up to
/// <c>/v1</c>, Ollama's say). Skipped without them, and not part of CI. .NET's providers are
/// <c>Microsoft.Extensions.AI</c>'s, so this checks Strata's side of one; Rust's <c>live.rs</c> also covers Anthropic,
/// whose client is Strata's own there.
/// </summary>
public sealed class ModelLiveFactAttribute : FactAttribute
{
    public ModelLiveFactAttribute()
    {
        if (ModelLive.Client() is null)
        {
            Skip = "STRATA_TEST_OPENAI_MODEL and STRATA_TEST_OPENAI_KEY or STRATA_TEST_OPENAI_URL not set (a live model; see design/44)";
        }
    }
}

internal static class ModelLive
{
    private static string? Var(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : null;

    /// <summary>The live client, with the SDK's own retries off, as design/44 advises.</summary>
    public static IChatClient? Client()
    {
        var model = Var("STRATA_TEST_OPENAI_MODEL");
        var url = Var("STRATA_TEST_OPENAI_URL");
        var key = Var("STRATA_TEST_OPENAI_KEY");
        if (model is null || (url is null && key is null))
        {
            return null;
        }

        var options = new OpenAIClientOptions { RetryPolicy = new ClientRetryPolicy(maxRetries: 0) };
        if (url is not null)
        {
            options.Endpoint = new Uri(url);
        }

        return new OpenAI.Chat.ChatClient(model, new ApiKeyCredential(key ?? "none"), options).AsIChatClient();
    }

    /// <summary>
    /// The live embedding generator (<c>design/45-Long-Term-Memory.md</c>, "Tests"): <c>STRATA_TEST_OPENAI_EMBEDDING_MODEL</c>
    /// on the same endpoint and key as <see cref="Client"/>, with the SDK's own retries off.
    /// </summary>
    public static IEmbeddingGenerator<string, Embedding<float>>? Embedder()
    {
        var model = Var("STRATA_TEST_OPENAI_EMBEDDING_MODEL");
        var url = Var("STRATA_TEST_OPENAI_URL");
        var key = Var("STRATA_TEST_OPENAI_KEY");
        if (model is null || (url is null && key is null))
        {
            return null;
        }

        var options = new OpenAIClientOptions { RetryPolicy = new ClientRetryPolicy(maxRetries: 0) };
        if (url is not null)
        {
            options.Endpoint = new Uri(url);
        }

        return new OpenAI.Embeddings.EmbeddingClient(model, new ApiKeyCredential(key ?? "none"), options).AsIEmbeddingGenerator();
    }
}

/// <summary>A live embedding test: skipped without <c>STRATA_TEST_OPENAI_EMBEDDING_MODEL</c> and a key or a URL.</summary>
public sealed class EmbeddingLiveFactAttribute : FactAttribute
{
    public EmbeddingLiveFactAttribute()
    {
        if (ModelLive.Embedder() is null)
        {
            Skip = "STRATA_TEST_OPENAI_EMBEDDING_MODEL and STRATA_TEST_OPENAI_KEY or STRATA_TEST_OPENAI_URL not set (a live embedding model; see design/45)";
        }
    }
}

public class ModelLiveTests
{
    public sealed record Ask(string Prompt, string Answer = "");

    [ModelLiveFact]
    public async Task Answers_A_Plain_Call()
    {
        using var chat = ModelLive.Client()!;

        var response = await StrataChat.GetResponseAsync(
            chat,
            [new(ChatRole.System, "Answer with one word."), new(ChatRole.User, "What colour is a clear daytime sky?")],
            new ChatOptions { MaxOutputTokens = 50 });

        Assert.False(string.IsNullOrWhiteSpace(response.Text));
        Assert.NotNull(response.FinishReason);
        Assert.True(response.Usage?.InputTokenCount > 0);
    }

    [ModelLiveFact]
    public async Task Streams_Through_A_Graph_Run()
    {
        using var chat = ModelLive.Client()!;
        var graph = new StateGraph<Ask>()
            .AddModelNode<Ask>(
                "Ask",
                chat,
                (ask, _) => [new(ChatRole.User, ask.Prompt)],
                (ask, response) => ask with { Answer = response.Text },
                new ModelNodeOptions { Chat = new ChatOptions { MaxOutputTokens = 100 } })
            .Compile();
        var channel = new RunEventChannel();

        var (reader, completion) = graph.RunStreaming(
            "live", new Ask("Count from one to five, in words."), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });
        var tokens = new System.Text.StringBuilder();
        await foreach (var chunk in reader.ReadAllAsync())
        {
            if (chunk.Type == StreamChunkType.Token)
            {
                tokens.Append(chunk.Payload);
            }
        }

        var completed = Assert.IsType<GraphExecutionResult<Ask>.Completed>(await completion);
        Assert.Equal(tokens.ToString(), completed.State.Answer);
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync())
        {
            events.Add(e);
        }

        var call = events.OfType<ModelCallCompleted>().Single();
        Assert.Equal(ModelCallResult.Succeeded, call.Result);
        Assert.True(call.Usage.OutputTokens > 0, "the streamed usage is reported");
    }

    public sealed record Report(string City, double Celsius);

    [ModelLiveFact]
    public async Task Runs_A_Tool_Loop_To_A_Typed_Answer()
    {
        using var chat = ModelLive.Client()!;
        var agent = new StrataAgent<Report>(chat, new ToolRegistry().Register(new LiveTools()), new StrataAgentOptions<Report>
        {
            Id = "weather-reporter",
            Chat = new ChatOptions { MaxOutputTokens = 400 },
        });
        AgentResult<Report>? produced = null;
        var graph = new StateGraph<Ask>()
            .AddAgentNode<Ask, Report>(
                "Report",
                agent,
                (ask, _) =>
                [
                    new(ChatRole.System, "Use the weather tool, then answer with one JSON object: the city and its temperature."),
                    new(ChatRole.User, ask.Prompt),
                ],
                (ask, result) =>
                {
                    produced = result;
                    return ask with { Answer = result.Output.City };
                })
            .Compile();

        var result = await graph.RunAsync("live-agent", new Ask("How warm is it in Lisbon now?"), new InMemoryCheckpointStore());

        Assert.IsType<GraphExecutionResult<Ask>.Completed>(result);
        Assert.Equal("weather.now", Assert.Single(produced!.ToolCalls).Name);
        Assert.Contains("Lisbon", produced.Output.City, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(17, produced.Output.Celsius);
        Assert.True(produced.Turns >= 2);
    }
}

/// <summary>The embedding call and the memory helper against a live embedding model (<c>design/45-Long-Term-Memory.md</c>).</summary>
public class EmbeddingLiveTests
{
    [EmbeddingLiveFact]
    public async Task Embeds_Two_Texts_In_One_Call_With_Its_Usage()
    {
        using var generator = ModelLive.Embedder()!;

        var embeddings = await StrataEmbeddings.GenerateAsync(generator, ["disk full on db-1", "pod in a crash loop"]);

        Assert.Equal(2, embeddings.Count);
        Assert.True(embeddings[0].Vector.Length > 0);
        Assert.Equal(embeddings[0].Vector.Length, embeddings[1].Vector.Length);
        Assert.True(embeddings.Usage?.InputTokenCount > 0, "the provider reports the input tokens");
    }

    [EmbeddingLiveFact]
    public async Task Remembers_And_Recalls_Through_An_In_Memory_Store()
    {
        using var generator = ModelLive.Embedder()!;
        var dimensions = (await StrataEmbeddings.GenerateAsync(generator, ["probe"]))[0].Vector.Length;
        var memory = new StrataMemory(new InMemoryMemoryStore(dimensions), generator, new StrataMemoryOptions { Model = "live" });

        await memory.RememberManyAsync(
        [
            new MemoryEntry("incidents", "disk", "The disk on db-1 is full and writes fail."),
            new MemoryEntry("incidents", "crash", "A pod restarts again and again in a crash loop."),
        ]);
        var recalled = await memory.RecallAsync("incidents", "no space left on the database volume");

        Assert.Equal(["disk", "crash"], recalled.Select(m => m.Record.Id));
        Assert.True(recalled[0].Similarity > recalled[1].Similarity);
    }
}

/// <summary>Tools for <see cref="ModelLiveTests"/>.</summary>
public partial class LiveTools
{
    [StrataTool(Name = "weather.now", Description = "The weather in a city now, as a temperature in degrees Celsius.")]
    public static string WeatherNow(string city) => $$"""{"city":"{{city}}","celsius":17}""";
}
