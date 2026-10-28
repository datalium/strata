using System.Diagnostics;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Observability;

/// <summary>
/// One tool call a registry dispatched, from <see cref="ToolCallStarted"/> to <see cref="ToolCallCompleted"/>
/// (<c>design/20-Tool-Dispatch-And-Registry.md</c>). Disposing it stops its activity; the outcome is
/// recorded once, by whichever of <see cref="Succeeded"/>, <see cref="Cancelled"/> or <see cref="Failed"/> comes first.
/// </summary>
internal sealed class ToolCallScope : IDisposable
{
    private readonly string _tool;
    private readonly bool _known;
    private readonly ToolCallContext _context;
    private readonly long _started = Stopwatch.GetTimestamp();
    private int _completed;

    /// <param name="known">Whether <paramref name="tool"/> is a name the registry knows, and so a bounded metric tag.</param>
    public ToolCallScope(string tool, bool known, ToolCallContext context)
    {
        _tool = tool;
        _known = known;
        _context = context;
        // A child of whatever is current (inside a node, its attempt's activity, or one the handler
        // started around the call), and of the context's activity when nothing is (a flow that
        // didn't carry Activity.Current).
        Activity = System.Diagnostics.Activity.Current is null && context.Activity is { } parent
            ? StrataTelemetry.Source.StartActivity("strata.tool", ActivityKind.Internal, parent.Context)
            : StrataTelemetry.Source.StartActivity("strata.tool", ActivityKind.Internal);
        if (Activity is not null)
        {
            Activity.DisplayName = $"execute_tool {tool}";
            Activity.SetTag("gen_ai.operation.name", "execute_tool");
            Activity.SetTag("gen_ai.tool.name", tool);
            Activity.SetTag("gen_ai.tool.call.id", context.CallId);
            Activity.SetTag("strata.thread_id", context.ThreadId);
            Activity.SetTag("strata.run_id", context.RunId);
            Activity.SetTag("strata.node.path", context.NodePath);
            Activity.SetTag("strata.attempt", context.Attempt);
            Activity.SetTag("strata.tool.caller", context.CallerId);
        }

        if (context.Telemetry is { Observed: true } run)
        {
            run.Record(new ToolCallStarted(tool, context.CallId ?? string.Empty, context.CallerId), context.NodePath, context.Attempt);
        }
    }

    /// <summary>The call's activity, the one the tool runs inside; <see langword="null"/> when nothing listens.</summary>
    public Activity? Activity { get; }

    public void Succeeded() => Complete(ToolCallResult.Succeeded, null, null);

    public void Cancelled() => Complete(ToolCallResult.Cancelled, null, null);

    /// <summary>The middleware chain returned without running the tool (<c>design/23-Tool-Middleware.md</c>).</summary>
    public void ShortCircuited() => Activity?.SetTag("strata.tool.short_circuited", true);

    public void Failed(Exception error)
    {
        var (result, errorType) = error switch
        {
            ToolNotPermittedException => (ToolCallResult.Refused, "not_permitted"),
            ToolNotFoundException => (ToolCallResult.Refused, "not_found"),
            ToolCallDeniedException => (ToolCallResult.Refused, "denied"),
            ToolArgumentsException => (ToolCallResult.Failed, "invalid_arguments"),
            _ => (ToolCallResult.Failed, error.GetType().FullName ?? error.GetType().Name),
        };
        Complete(result, error.Message, errorType);
    }

    public void Dispose() => Activity?.Dispose();

    private void Complete(ToolCallResult result, string? error, string? errorType)
    {
        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            return;
        }

        var elapsed = Stopwatch.GetElapsedTime(_started);
        var resultTag = result switch
        {
            ToolCallResult.Succeeded => "succeeded",
            ToolCallResult.Failed => "failed",
            ToolCallResult.Cancelled => "cancelled",
            _ => "refused",
        };
        var tags = _known
            ? new TagList { { "gen_ai.tool.name", _tool }, { "strata.tool.result", resultTag } }
            // The name came from the model, so it can't be a metric tag.
            : new TagList { { "strata.tool.result", resultTag } };
        StrataTelemetry.ToolRequests.Add(1, tags);
        StrataTelemetry.ToolDuration.Record(elapsed.TotalSeconds, tags);

        Activity?.SetTag("strata.tool.result", resultTag);
        if (error is not null)
        {
            StrataTelemetry.Fail(Activity, error, errorType);
        }

        if (_context.Telemetry is { Observed: true } run)
        {
            run.Record(
                new ToolCallCompleted(_tool, _context.CallId ?? string.Empty, result, elapsed, error),
                _context.NodePath,
                _context.Attempt);
        }
    }
}
