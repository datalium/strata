using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// One scripted answer: the text chunks to stream (one update each), the tool calls to ask for (id, name, and the
/// arguments as the model's JSON text), the usage and finish reason to report, and optionally a failure, thrown before
/// anything (<see cref="FailAfter"/> 0) or after that many chunks.
/// </summary>
public sealed record Scripted(IReadOnlyList<string> Chunks, long? InputTokens = 12, long? OutputTokens = 5, string FinishReason = "stop")
{
    public IReadOnlyList<(string Id, string Name, string Arguments)> ToolCalls { get; init; } = [];

    public Exception? Failure { get; init; }

    public int FailAfter { get; init; }

    /// <summary>Waits until the caller's token is cancelled, before anything.</summary>
    public bool Hang { get; init; }

    public static Scripted Text(params string[] chunks) => new(chunks);

    /// <summary>An answer asking for these tool calls.</summary>
    public static Scripted Tools(params (string Id, string Name, string Arguments)[] calls) =>
        new([], FinishReason: "tool_calls") { ToolCalls = calls };

    /// <summary>This answer, also asking for a tool call.</summary>
    public Scripted WithToolCall(string id, string name, string arguments) =>
        this with { ToolCalls = [.. ToolCalls, (id, name, arguments)], FinishReason = "tool_calls" };

    public static Scripted Fails(Exception failure, int afterChunks = 0) => new([]) { Failure = failure, FailAfter = afterChunks };
}

/// <summary>
/// A chat client that answers from a script, in order, and records every request: the tests' model in place of a
/// provider (<c>design/44-LLM-Integration.md</c>, "Tests"). Mirrors the Rust suite's <c>ScriptedChatClient</c>.
/// </summary>
public sealed class ScriptedChatClient(params Scripted[] script) : IChatClient
{
    private readonly ConcurrentQueue<Scripted> _script = new(script);

    public const string Model = "scripted-model";

    public ConcurrentQueue<(IReadOnlyList<ChatMessage> Messages, ChatOptions? Options, bool Streamed)> Requests { get; } = [];

    public async Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        var step = Next(messages, options, streamed: false);
        if (step.Hang)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        if (step.Failure is { } failure)
        {
            throw failure;
        }

        List<AIContent> contents = step.Chunks.Count == 0 ? [] : [new TextContent(string.Concat(step.Chunks))];
        contents.AddRange(step.ToolCalls.Select(Call));
        return new ChatResponse(new ChatMessage(ChatRole.Assistant, contents))
        {
            ModelId = options?.ModelId ?? Model,
            ResponseId = "resp-1",
            FinishReason = new ChatFinishReason(step.FinishReason),
            Usage = new UsageDetails { InputTokenCount = step.InputTokens, OutputTokenCount = step.OutputTokens },
        };
    }

    public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var step = Next(messages, options, streamed: true);
        if (step.Hang)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        for (var i = 0; i < step.Chunks.Count || (step.Failure is not null && i == step.FailAfter); i++)
        {
            if (step.Failure is { } failure && i == step.FailAfter)
            {
                throw failure;
            }

            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, step.Chunks[i]) { ModelId = options?.ModelId ?? Model, ResponseId = "resp-1" };
        }

        foreach (var call in step.ToolCalls)
        {
            yield return new ChatResponseUpdate(ChatRole.Assistant, [Call(call)]) { ResponseId = "resp-1" };
        }

        yield return new ChatResponseUpdate(ChatRole.Assistant, [new UsageContent(new UsageDetails
        {
            InputTokenCount = step.InputTokens,
            OutputTokenCount = step.OutputTokens,
        })])
        {
            FinishReason = new ChatFinishReason(step.FinishReason),
            ResponseId = "resp-1",
        };
    }

    public object? GetService(Type serviceType, object? serviceKey = null) =>
        serviceType == typeof(ChatClientMetadata) ? new ChatClientMetadata("scripted", null, Model) : null;

    public void Dispose()
    {
    }

    /// <summary>A tool call as <c>Microsoft.Extensions.AI</c>'s providers parse one: arguments that aren't a JSON object set its <c>Exception</c>.</summary>
    private static FunctionCallContent Call((string Id, string Name, string Arguments) call) =>
        FunctionCallContent.CreateFromParsedArguments(
            call.Arguments,
            call.Id,
            call.Name,
            static json => JsonSerializer.Deserialize<Dictionary<string, object?>>(json)!);

    private Scripted Next(IEnumerable<ChatMessage> messages, ChatOptions? options, bool streamed)
    {
        Requests.Enqueue(([.. messages], options, streamed));
        return _script.TryDequeue(out var step) ? step : throw new InvalidOperationException("the script has no more answers");
    }
}
