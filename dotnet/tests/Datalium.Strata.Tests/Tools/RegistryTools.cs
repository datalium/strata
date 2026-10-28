using System.Collections.Concurrent;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests.Tools;

/// <summary>Three tools for the registry suites: the twins of <c>tool_registry.rs</c>'s <c>alpha</c> / <c>beta</c> / <c>gamma</c>.</summary>
public partial class AbcTools
{
    [StrataTool(Name = "alpha", Description = "First.")]
    public static string Alpha(int x) => $"alpha {x}";

    [StrataTool(Name = "beta", Description = "Second.")]
    public static string Beta(int x) => $"beta {x}";

    [StrataTool(Name = "gamma", Description = "Third.")]
    public static string Gamma(int x) => $"gamma {x}";
}

/// <summary>A second tool named <c>alpha</c>, as another provider might declare one.</summary>
public partial class OtherAlphaTools
{
    [StrataTool(Name = "alpha", Description = "Another alpha.")]
    public static string OtherAlpha(int x) => $"other alpha {x}";
}

/// <summary>
/// Tools that take the call's context and token, on a provider with a constructor dependency (tool
/// methods may be instance methods). The context and token parameters sit in different positions.
/// </summary>
public partial class ContextTools(ContextTools.Probe probe)
{
    /// <summary>What the tools saw, and a signal that <c>wait</c> started.</summary>
    public sealed class Probe
    {
        private int _calls;

        public ConcurrentQueue<ToolCallContext> Seen { get; } = new();

        public int Calls => Volatile.Read(ref _calls);

        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Called() => Interlocked.Increment(ref _calls);
    }

    [StrataTool(Name = "probe", Description = "Records its context.")]
    public string Record(ToolCallContext context)
    {
        probe.Called();
        probe.Seen.Enqueue(context);
        return "probed";
    }

    [StrataTool(Name = "wait", Description = "Waits for its token.")]
    public async Task<string> WaitAsync(CancellationToken cancellationToken, ToolCallContext context)
    {
        probe.Called();
        probe.Seen.Enqueue(context);
        probe.Started.TrySetResult();
        await Task.Delay(Timeout.Infinite, cancellationToken);
        return "never";
    }

    [StrataTool(Name = "mixed", Description = "JSON arguments around the context and the token.")]
    public string Mixed(string sku, CancellationToken cancellationToken, int count, ToolCallContext context) =>
        $"{sku} x{count} for {context.CallerId} ({(cancellationToken.CanBeCanceled ? "cancellable" : "not cancellable")})";

    [StrataTool(Name = "plain", Description = "Takes no context.")]
    public static string Plain() => "plain";

    [StrataTool(Name = "foreign_cancel", Description = "Throws an OperationCanceledException nobody asked for.")]
    public static string ForeignCancel() => throw new OperationCanceledException("the HTTP client timed out");
}

/// <summary>A provider written by hand before <see cref="ToolCallContext"/> existed: only the two-argument dispatch.</summary>
public sealed class LegacyProvider : IStrataToolProvider
{
    public string GetToolsManifest() =>
        """[{"name":"legacy","description":"Old.","parameters":{"type":"object","properties":{},"required":[]}}]""";

    public Task<string> DispatchToolAsync(string toolName, string jsonArguments) =>
        toolName == "legacy" ? Task.FromResult("legacy") : throw new ToolNotFoundException(toolName);
}
