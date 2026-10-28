using Datalium.Strata.Graph;
using Datalium.Strata.Observability;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>What <see cref="StrataChat"/> needs besides the request itself.</summary>
public sealed record StrataChatOptions
{
    /// <summary>Reads a provider's own exceptions before the default does (<see cref="ModelFaults.Classify"/>).</summary>
    public IModelFaultClassifier? FaultClassifier { get; init; }

    /// <summary>The agent making the call, for the span's <c>strata.agent</c>.</summary>
    public string? Agent { get; init; }
}

/// <summary>
/// A model call from a node (<c>design/44-LLM-Integration.md</c>): streamed when the run streams, observed either way,
/// and failed the way a node fails. Everything else in <c>Datalium.Strata.AI</c> calls models through it.
/// </summary>
public static class StrataChat
{
    /// <summary>
    /// Asks <paramref name="client"/> for an answer to <paramref name="messages"/>.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item>When <paramref name="context"/> has a stream (the run is streaming), the call streams and each update's
    /// text goes to it as a <c>Token</c> chunk; otherwise it is one request. Either way the whole answer comes back.</item>
    /// <item>It is recorded through a <see cref="ModelCallScope"/>: <see cref="ModelCallStarted"/> and
    /// <see cref="ModelCallCompleted"/> with the provider's usage, a <c>chat &lt;model&gt;</c> span and the GenAI
    /// metrics. The model is <paramref name="options"/>' <c>ModelId</c>, else the client's default, else <c>unknown</c>.</item>
    /// <item>A fault throws a <see cref="NodeException"/>: transient with the provider's retry hint, or fatal
    /// (<see cref="ModelFaults"/>). A stream that breaks after its first update is transient. The node's own
    /// cancellation is rethrown as it is.</item>
    /// <item>A <see cref="ModelRoute"/> isn't one call (<c>design/46-Model-Routes.md</c>): each of its attempts is a model
    /// call of its own under a <c>route &lt;name&gt;</c> span, the route records <see cref="ModelRouted"/> once, and only the
    /// winner's text goes to the stream. The route's failure (<see cref="ModelRouteException"/>) fails the node with its
    /// message, transient with its hint or fatal; a candidate's fatal fault fails it as any call's would. Only a client
    /// that is itself a route is routed so: one wrapped in another client is called as that client.</item>
    /// </list>
    /// </remarks>
    public static async Task<ChatResponse> GetResponseAsync(
        IChatClient client,
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        NodeContext? context = null,
        StrataChatOptions? strata = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(messages);
        if (client is ModelRoute route)
        {
            return await route.CompleteAsync(messages, options, context, strata, cancellationToken).ConfigureAwait(false);
        }

        var metadata = client.GetService<ChatClientMetadata>();
        var model = ModelOf(options?.ModelId, metadata?.DefaultModelId);
        using var scope = ModelCallScope.Start(context, new ModelCallInfo(model, metadata?.ProviderName)
        {
            MaxTokens = options?.MaxOutputTokens,
            Temperature = options?.Temperature,
            Agent = strata?.Agent,
        });

        var stream = context?.Stream;
        var started = false;
        try
        {
            ChatResponse response;
            if (stream is null)
            {
                response = await client.GetResponseAsync(messages, options, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var updates = new List<ChatResponseUpdate>();
                await foreach (var update in client.GetStreamingResponseAsync(messages, options, cancellationToken)
                    .WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    if (!started)
                    {
                        started = true;
                        scope.FirstChunk();
                    }

                    updates.Add(update);
                    var text = update.Text;
                    if (!string.IsNullOrEmpty(text))
                    {
                        await stream.EmitTokenAsync(text, cancellationToken).ConfigureAwait(false);
                    }
                }

                response = updates.ToChatResponse();
            }

            scope.Succeeded(Usage(response.Usage), response.FinishReason?.Value, response.ModelId, response.ResponseId);
            return response;
        }
        catch (Exception exception)
        {
            if (Settle(scope, exception, strata?.FaultClassifier, started, cancellationToken) is { } failure)
            {
                throw failure;
            }

            throw;
        }
    }

    /// <summary>
    /// Records how a model call that threw <paramref name="exception"/> ended, and returns the node failure to throw
    /// in its place, or <see langword="null"/> to rethrow it as it is: the node's own cancellation (recorded
    /// <see cref="ModelCallScope.Cancelled"/>), and a <see cref="NodeException"/> from a client that already speaks
    /// Strata's language. Anything else is classified (<see cref="ModelFaults.Classify"/>); after a stream's first
    /// update, as transient <c>stream_broken</c>. Shared by the chat call and the embedding call
    /// (<see cref="StrataEmbeddings"/>).
    /// </summary>
    internal static NodeException? Settle(
        ModelCallScope scope,
        Exception exception,
        IModelFaultClassifier? classifier,
        bool started,
        CancellationToken cancellationToken)
    {
        if (exception is OperationCanceledException && cancellationToken.IsCancellationRequested)
        {
            scope.Cancelled();
            return null;
        }

        if (exception is NodeException failure)
        {
            scope.Failed(failure.Kind == NodeFailureKind.Transient ? "transient" : "fatal", failure.Message);
            return null;
        }

        var fault = ModelFaults.Classify(exception, classifier);
        if (started)
        {
            // Some of the answer has been streamed: whatever broke it, a new attempt is what can finish it.
            fault = fault with { IsTransient = true, ErrorType = "stream_broken" };
        }

        scope.Failed(fault.ErrorType, exception.Message);
        return ModelFaults.ToNodeException(fault, exception);
    }

    /// <summary>
    /// The model a call names: <paramref name="asked"/>, else the client's <paramref name="defaultModel"/>, else
    /// <c>unknown</c>; a blank name is no name.
    /// </summary>
    internal static string ModelOf(string? asked, string? defaultModel) =>
        !string.IsNullOrWhiteSpace(asked) ? asked
        : !string.IsNullOrWhiteSpace(defaultModel) ? defaultModel
        : "unknown";

    /// <summary>The usage <paramref name="usage"/> reports, as the engine's events carry it.</summary>
    public static ModelUsage Usage(UsageDetails? usage) =>
        usage is null ? ModelUsage.None : new ModelUsage(usage.InputTokenCount, usage.OutputTokenCount);
}
