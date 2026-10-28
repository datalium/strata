using Datalium.Strata.Graph;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>A model node's options (<see cref="ModelNodes.AddModelNode"/>).</summary>
public sealed record ModelNodeOptions
{
    /// <summary>The request's options (model, temperature, …), copied for every call.</summary>
    public ChatOptions? Chat { get; init; }

    /// <summary>Reads a provider's own exceptions before the default does.</summary>
    public IModelFaultClassifier? FaultClassifier { get; init; }

    /// <summary>The node's description-only metadata (<c>design/22</c>).</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }
}

/// <summary>The model node (<c>design/44-LLM-Integration.md</c>): one model call as a node of a graph.</summary>
public static class ModelNodes
{
    /// <summary>
    /// Adds a fallible async node that builds messages from the state (<paramref name="prompt"/>), makes one call
    /// through <see cref="StrataChat"/>, and folds the answer into the state (<paramref name="apply"/>). A transient
    /// fault fails the attempt transiently, so <c>SetRecoveryPolicy</c> with a backoff retries it after the
    /// provider's hint (<c>design/25</c>).
    /// </summary>
    public static StateGraph<TState> AddModelNode<TState>(
        this StateGraph<TState> graph,
        string name,
        IChatClient client,
        Func<TState, NodeContext, IEnumerable<ChatMessage>> prompt,
        Func<TState, ChatResponse, TState> apply,
        ModelNodeOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(prompt);
        ArgumentNullException.ThrowIfNull(apply);
        var strata = new StrataChatOptions { FaultClassifier = options?.FaultClassifier };
        return graph.AddFallibleNode(
            name,
            async (state, context, cancellationToken) =>
            {
                var response = await StrataChat.GetResponseAsync(
                    client, prompt(state, context), options?.Chat?.Clone(), context, strata, cancellationToken).ConfigureAwait(false);
                return apply(state, response);
            },
            options?.Metadata);
    }
}
