using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Datalium.Strata.Observability;

/// <summary>
/// The engine's OpenTelemetry-compatible instrumentation (<c>design/18-Run-Observability.md</c>): one
/// <see cref="ActivitySource"/> and one <see cref="Meter"/>, both named <see cref="Name"/>. Listen with
/// <c>AddSource(StrataTelemetry.Name)</c> / <c>AddMeter(StrataTelemetry.Name)</c>; nothing is recorded
/// until something does.
/// </summary>
public static class StrataTelemetry
{
    /// <summary>The name of the engine's <see cref="ActivitySource"/> and <see cref="Meter"/>.</summary>
    public const string Name = "Datalium.Strata";

    private static readonly string? Version = typeof(StrataTelemetry).Assembly.GetName().Version?.ToString();

    internal static readonly ActivitySource Source = new(Name, Version);

    internal static readonly Meter Meter = new(Name, Version);

    internal static readonly Counter<long> GraphsStarted = Meter.CreateCounter<long>(
        "strata.graphs.started", "{run}", "Runs and resumes started.");

    internal static readonly UpDownCounter<long> GraphsActive = Meter.CreateUpDownCounter<long>(
        "strata.graphs.active", "{run}", "Runs and resumes executing in this process.");

    internal static readonly Histogram<double> GraphDuration = Meter.CreateHistogram<double>(
        "strata.graph.duration", "s", "How long a run or resume took, by outcome.");

    internal static readonly Histogram<double> NodeDuration = Meter.CreateHistogram<double>(
        "strata.node.duration", "s", "How long a node took, every attempt included, by result.");

    internal static readonly Counter<long> NodeRetries = Meter.CreateCounter<long>(
        "strata.node.retries", "{retry}", "Attempts that failed transiently and were retried.");

    internal static readonly Counter<long> Transitions = Meter.CreateCounter<long>(
        "strata.graph.transition", "{transition}", "Edges followed from one node to the next.");

    internal static readonly Counter<long> HopLimitsExceeded = Meter.CreateCounter<long>(
        "strata.graph.hop_limit_exceeded", "{run}", "Runs a graph level's hop limit stopped, by the node refused.");

    internal static readonly Counter<long> Wakes = Meter.CreateCounter<long>(
        "strata.thread.wakes", "{thread}", "Waiting threads a signal or the wake driver woke or ended, by reason.");

    internal static readonly Counter<long> Recoveries = Meter.CreateCounter<long>(
        "strata.thread.recoveries", "{thread}", "Running threads a recovery claimed and continued.");

    internal static readonly Counter<long> LeaseAcquisitions = Meter.CreateCounter<long>(
        "strata.lease.acquisitions", "{attempt}", "Thread leases a run tried to acquire, by outcome (acquired or held).");

    internal static readonly Counter<long> LeaseLosses = Meter.CreateCounter<long>(
        "strata.lease.losses", "{run}", "Runs that lost their thread's lease and stopped.");

    internal static readonly Histogram<double> CheckpointLatency = Meter.CreateHistogram<double>(
        "strata.checkpoint.latency", "s", "How long a checkpoint save or load took.");

    internal static readonly Counter<long> ObserverErrors = Meter.CreateCounter<long>(
        "strata.observer.errors", "{error}", "Exceptions thrown by run observers, which never fail the run.");

    internal static readonly Counter<long> ToolRequests = Meter.CreateCounter<long>(
        "strata.tool.requests", "{request}", "Tool calls dispatched, by result.");

    internal static readonly Histogram<double> ToolDuration = Meter.CreateHistogram<double>(
        "strata.tool.duration", "s", "How long a tool call took, by result.");

    internal static readonly Histogram<long> ModelTokenUsage = Meter.CreateHistogram<long>(
        "gen_ai.client.token.usage", "{token}", "Tokens a model call used, by token type (input or output).");

    internal static readonly Histogram<double> ModelDuration = Meter.CreateHistogram<double>(
        "gen_ai.client.operation.duration", "s", "How long a model call took.");

    internal static readonly Histogram<double> ModelTimeToFirstChunk = Meter.CreateHistogram<double>(
        "strata.model.time_to_first_chunk", "s", "How long a streamed model call took to send its first update.");

    internal static readonly Counter<long> RouteCalls = Meter.CreateCounter<long>(
        "strata.route.calls", "{call}", "Calls through a model route, by route and outcome.");

    internal static readonly Counter<long> RouteFailovers = Meter.CreateCounter<long>(
        "strata.route.failovers", "{candidate}", "Candidates a model route moved past, by route and reason.");

    internal static readonly Counter<long> RouteHedges = Meter.CreateCounter<long>(
        "strata.route.hedges", "{attempt}", "Candidates a model route started beside a slow one, by route.");

    internal static readonly Counter<long> BreakerTransitions = Meter.CreateCounter<long>(
        "strata.model.breaker.transitions", "{transition}", "A model health key's breaker opening, half-opening or closing.");

    internal static readonly Counter<long> SchedulerAdmissions = Meter.CreateCounter<long>(
        "strata.scheduler.admissions", "{run}", "Runs a scheduler admitted or refused, by outcome and priority.");

    internal static readonly Histogram<double> SchedulerWait = Meter.CreateHistogram<double>(
        "strata.scheduler.wait", "s", "How long a run waited for a scheduler's slot, admitted or not, by outcome.");

    internal static readonly UpDownCounter<long> SchedulerQueued = Meter.CreateUpDownCounter<long>(
        "strata.scheduler.queued", "{run}", "Runs waiting for a scheduler's slot.");

    internal static readonly UpDownCounter<long> SchedulerActive = Meter.CreateUpDownCounter<long>(
        "strata.scheduler.active", "{run}", "Runs holding a scheduler's slot.");

    /// <summary>Marks <paramref name="activity"/> failed, if there is one.</summary>
    internal static void Fail(Activity? activity, string message, string? errorType)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, message);
        if (errorType is not null)
        {
            activity.SetTag("error.type", errorType);
        }
    }
}
