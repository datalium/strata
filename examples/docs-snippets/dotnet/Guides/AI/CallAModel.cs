// Samples for https://stratamesh.dev/guides/ai/call-a-model/. A stand-in model, AnsweringChatClient, answers every
// call, so the tests need no provider.

// docs:begin guides/ai/call-a-model/create-client
using System.ClientModel;
using System.ClientModel.Primitives;
// docs:hide
using System.Runtime.CompilerServices;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;
// docs:show
using Microsoft.Extensions.AI;
using OpenAI;

// docs:hide
namespace Datalium.Strata.DocsSnippets.Guides.AI.CallAModel;

[Collection(ConsoleOutput.Collection)]
public class CallAModelSamples
{
    // docs:show
    static IChatClient CreateOpenAIClient(string model, string? apiKey, string? baseUrl) =>
        new OpenAI.Chat.ChatClient(
            model,
            new ApiKeyCredential(apiKey ?? "not-needed"),
            new OpenAIClientOptions
            {
                RetryPolicy = new ClientRetryPolicy(maxRetries: 0),
                Endpoint = baseUrl is null ? null : new Uri(baseUrl),
            })
        .AsIChatClient();
    // docs:end guides/ai/call-a-model/create-client

    private const string Question = "What is a borrow checker?";

    private const string Answer =
        "A borrow checker is the part of the Rust compiler that checks references. " +
        "It makes sure no reference outlives its value, at compile time.";

    [Fact]
    public void CreateAChatClient()
    {
        // Endpoint left null: OpenAI itself.
        using var openAI = CreateOpenAIClient("gpt-4o-mini", "sk-not-a-real-key", baseUrl: null);
        var metadata = openAI.GetService<ChatClientMetadata>();
        Assert.Equal("gpt-4o-mini", metadata?.DefaultModelId);
        Assert.Equal("api.openai.com", metadata?.ProviderUri?.Host);

        // Another OpenAI-compatible server, with no key.
        using var local = CreateOpenAIClient("qwen2.5:3b", apiKey: null, "http://localhost:11434/v1");
        metadata = local.GetService<ChatClientMetadata>();
        Assert.Equal("qwen2.5:3b", metadata?.DefaultModelId);
        Assert.Equal(new Uri("http://localhost:11434/v1"), metadata?.ProviderUri);
    }

    [Fact]
    public async Task ModelNode()
    {
        var client = new AnsweringChatClient(Answer);

        var result = await BuildGraph(client).RunAsync(
            "call-a-model-model-node", new LlmStreamState { Question = Question }, new InMemoryCheckpointStore());

        var completed = Assert.IsType<GraphExecutionResult<LlmStreamState>.Completed>(result);
        Assert.Equal(Answer, completed.State.Answer);
        Assert.Equal(Answer.Length, completed.State.AnswerCharCount);
        // One request, with the node's options, that waited for the whole answer.
        var request = Assert.Single(client.Requests);
        Assert.Equal(200, request.Options?.MaxOutputTokens);
        Assert.False(request.Streamed);
    }

    [Fact]
    public async Task OwnNode()
    {
        var answering = new AnsweringChatClient(Answer);
        IChatClient client = answering;

        var graph = new StateGraph<LlmStreamState>()
            // docs:begin guides/ai/call-a-model/own-node
            .AddFallibleNode("AnswerQuestion", async (state, context, cancellationToken) =>
            {
                var response = await StrataChat.GetResponseAsync(
                    client,
                    [new(ChatRole.User, state.Question)],
                    new ChatOptions { MaxOutputTokens = 200 },
                    context,
                    cancellationToken: cancellationToken);
                return state with { Answer = response.Text };
            })
            // docs:end guides/ai/call-a-model/own-node
            .Compile();

        var result = await graph.RunAsync(
            "call-a-model-own-node", new LlmStreamState { Question = Question }, new InMemoryCheckpointStore());

        var completed = Assert.IsType<GraphExecutionResult<LlmStreamState>.Completed>(result);
        Assert.Equal(Answer, completed.State.Answer);
        Assert.Equal(200, Assert.Single(answering.Requests).Options?.MaxOutputTokens);
    }

    [Fact]
    public async Task StreamTheAnswer()
    {
        using var console = ConsoleOutput.Capture();
        var client = new AnsweringChatClient(Answer);

        // docs:begin guides/ai/call-a-model/stream
        var (reader, completion) = BuildGraph(client).RunStreaming(
            "stream-thread", new LlmStreamState { Question = "What is a borrow checker?" }, new InMemoryCheckpointStore());

        await foreach (var chunk in reader.ReadAllAsync())
        {
            switch (chunk.Type)
            {
                case StreamChunkType.Token:
                    Console.Write(chunk.Payload.ToString());
                    break;
                case StreamChunkType.Retry:
                    Console.WriteLine($"\n[retry] {chunk.NodeName}");
                    break;
                case StreamChunkType.Error:
                    Console.WriteLine($"\n[error] {chunk.NodeName}: {chunk.Payload}");
                    break;
            }
        }

        var result = await completion;
        if (result is GraphExecutionResult<LlmStreamState>.Completed done)
        {
            Console.WriteLine($"\n\n{done.State.AnswerCharCount} characters");
        }
        // docs:end guides/ai/call-a-model/stream

        Assert.IsType<GraphExecutionResult<LlmStreamState>.Completed>(result);
        // The Token chunks, joined in order, are the whole answer.
        Assert.Equal($"{Answer}\n\n{Answer.Length} characters\n", console.Text);
        // Because the run streamed, the call streamed too.
        Assert.True(Assert.Single(client.Requests).Streamed);
    }

    // docs:begin guides/ai/call-a-model/model-node
    static CompiledGraph<LlmStreamState> BuildGraph(IChatClient client) =>
        new StateGraph<LlmStreamState>()
            .AddModelNode<LlmStreamState>(
                "AnswerQuestion",
                client,
                (state, _) =>
                [
                    new(ChatRole.System, "You are a helpful assistant. Answer in two or three sentences."),
                    new(ChatRole.User, state.Question),
                ],
                (state, response) => state with { Answer = response.Text },
                new ModelNodeOptions { Chat = new ChatOptions { MaxOutputTokens = 200 } })
            .SetRecoveryPolicy("AnswerQuestion", RecoveryPolicy.Retry(2).WithBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10)))
            .AddNode("SummarizeAnswer", state => state with { AnswerCharCount = state.Answer.Length })
            .AddEdge("AnswerQuestion", "SummarizeAnswer")
            .Compile();

    // docs:hide
}

// docs:show
sealed record LlmStreamState
{
    public required string Question { get; init; }
    public string Answer { get; init; } = string.Empty;
    public int AnswerCharCount { get; init; }
}
// docs:end guides/ai/call-a-model/model-node

/// <summary>
/// A stand-in for a model: answers every request with <paramref name="answer"/>, streamed a word at a time, and
/// records each request's options and whether it streamed.
/// </summary>
internal sealed class AnsweringChatClient(string answer) : IChatClient
{
    public List<(ChatOptions? Options, bool Streamed)> Requests { get; } = [];

    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        Requests.Add((options, false));
        return Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, answer)) { FinishReason = ChatFinishReason.Stop });
    }

    public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Requests.Add((options, true));
        var words = answer.Split(' ');
        for (var i = 0; i < words.Length; i++)
        {
            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, i < words.Length - 1 ? words[i] + " " : words[i]);
        }
    }

    public object? GetService(Type serviceType, object? serviceKey = null) => null;

    public void Dispose()
    {
    }
}

/// <summary>
/// What the samples print, read back by their tests. Console.Out is the process's, so the tests that read it are in
/// one collection that runs alone.
/// </summary>
internal sealed class ConsoleOutput : IDisposable
{
    public const string Collection = "guides/ai/call-a-model console";

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
