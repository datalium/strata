using Datalium.Strata.Graph;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>The agent node (<c>design/44-LLM-Integration.md</c>): an agent's run as a node of a graph.</summary>
public static class AgentNodes
{
    /// <summary>
    /// Adds a fallible async node that runs <paramref name="agent"/> on the messages <paramref name="prompt"/> builds
    /// from the state, and folds its result into the state (<paramref name="apply"/>). Sugar over
    /// <see cref="StrataAgent{TOutput}.RunAsync"/>, which a node can call itself; the agent is not a node kind, and the
    /// node exports like any.
    /// </summary>
    public static StateGraph<TState> AddAgentNode<TState, TOutput>(
        this StateGraph<TState> graph,
        string name,
        StrataAgent<TOutput> agent,
        Func<TState, NodeContext, IEnumerable<ChatMessage>> prompt,
        Func<TState, AgentResult<TOutput>, TState> apply,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(agent);
        ArgumentNullException.ThrowIfNull(prompt);
        ArgumentNullException.ThrowIfNull(apply);
        return graph.AddFallibleNode(
            name,
            async (state, context, cancellationToken) =>
            {
                var result = await agent.RunAsync(prompt(state, context), context, cancellationToken).ConfigureAwait(false);
                return apply(state, result);
            },
            metadata);
    }
}
