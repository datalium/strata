using System.Diagnostics;
using Datalium.Strata.Graph;
using Datalium.Strata.Observability;

namespace Datalium.Strata.Tools;

/// <summary>
/// What a tool call is told about its caller (<c>design/20-Tool-Dispatch-And-Registry.md</c>). Every
/// property is optional, so a tool can be called outside any run (<see cref="None"/>). Build one from
/// a node's context with <see cref="From(NodeContext)"/>, then add the caller:
/// <c>ToolCallContext.From(ctx) with { CallerId = "kubernetes-investigator", CallId = call.Id }</c>.
/// The cancellation token is the separate <see cref="CancellationToken"/> argument of
/// <see cref="IStrataToolProvider.DispatchToolAsync(string, string, ToolCallContext, CancellationToken)"/>.
/// Mirrors the Rust engine's <c>ToolCallContext</c>.
/// </summary>
/// <remarks>
/// A record (unlike <see cref="NodeContext"/>) so the caller can derive one with <c>with</c>; adding an
/// <c>init</c> property to it is non-breaking. A <c>[StrataTool]</c> method receives it by declaring a
/// <see cref="ToolCallContext"/> parameter, which is left out of the tool's manifest.
/// </remarks>
public sealed record ToolCallContext
{
    /// <summary>Nothing known: a call outside any run.</summary>
    public static ToolCallContext None { get; } = new();

    /// <summary>The thread id of the run the call is made from, at every depth.</summary>
    public string? ThreadId { get; init; }

    /// <summary>The id of the run the call is made from.</summary>
    public string? RunId { get; init; }

    /// <summary>The calling node's qualified path (<c>"Work/Gather/Acme"</c>).</summary>
    public string? NodePath { get; init; }

    /// <summary>The calling node's attempt.</summary>
    public int? Attempt { get; init; }

    /// <summary>The run's metadata (<c>tenant</c>, <c>incident</c>, …). Empty when there is none.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = RunMetadata.None;

    /// <summary>Who is calling, typically the agent's identity. Opaque to Strata.</summary>
    public string? CallerId { get; init; }

    /// <summary>
    /// The model's tool-call id. A <see cref="ToolRegistry"/> generates one (a UUID v7) when the caller
    /// didn't, so a tool it dispatches always has one.
    /// </summary>
    public string? CallId { get; init; }

    /// <summary>
    /// The span the call runs inside: the tool's <c>strata.tool</c> span when a
    /// <see cref="ToolRegistry"/> dispatched it, otherwise what the caller supplied (a node's attempt
    /// span, via <see cref="From(NodeContext)"/>). <see langword="null"/> when nothing listens to
    /// <c>Datalium.Strata</c>.
    /// </summary>
    public Activity? Activity { get; init; }

    /// <summary>What the run's <see cref="RunOptions.Services"/> supplied, if anything.</summary>
    public IServiceProvider? Services { get; init; }

    /// <summary>The run's hook layer, which puts a call's events in its run's event stream.</summary>
    internal RunTelemetry? Telemetry { get; init; }

    /// <summary>
    /// Everything <paramref name="node"/> knows: thread id, run id, node path, attempt, metadata, its
    /// span and services. The caller and call ids aren't a node's to know.
    /// </summary>
    public static ToolCallContext From(NodeContext node)
    {
        ArgumentNullException.ThrowIfNull(node);
        return new ToolCallContext
        {
            ThreadId = node.ThreadId,
            RunId = string.IsNullOrEmpty(node.RunId) ? null : node.RunId,
            NodePath = node.NodePath,
            Attempt = node.Attempt,
            Metadata = node.Metadata,
            Activity = node.Activity,
            Services = node.Services,
            Telemetry = node.Telemetry,
        };
    }
}
