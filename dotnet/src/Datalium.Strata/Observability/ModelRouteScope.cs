using System.Diagnostics;
using Datalium.Strata.Graph;

namespace Datalium.Strata.Observability;

/// <summary>
/// One call through a model route (<c>design/46-Model-Routes.md</c>), from its start to <see cref="ModelRouted"/>: the
/// way <c>Datalium.Strata.AI</c>'s <c>ModelRoute</c> (or an application's own router) puts the call in its run's events
/// and the engine's telemetry, as <see cref="ModelCallScope"/> does for each attempt. It opens a <c>strata.route</c>
/// activity named <c>route &lt;name&gt;</c>, which the attempts' <c>strata.model</c> activities are children of, and
/// records the <c>strata.route.*</c> metrics.
/// </summary>
/// <remarks>
/// <see cref="Completed"/> records the outcome once; disposing stops the activity. Started without a node
/// (<see langword="null"/>), it records no run event.
/// </remarks>
public sealed class ModelRouteScope : IDisposable
{
    private readonly NodeContext? _node;
    private int _completed;

    private ModelRouteScope(NodeContext? node, string route)
    {
        _node = node;
        Route = route;
        CallId = Guid.CreateVersion7().ToString();
        // A child of whatever is current (inside a node, its attempt's activity), and of the node's activity when
        // nothing is, as a model call's.
        Activity = System.Diagnostics.Activity.Current is null && node?.Activity is { } parent
            ? StrataTelemetry.Source.StartActivity("strata.route", ActivityKind.Internal, parent.Context)
            : StrataTelemetry.Source.StartActivity("strata.route", ActivityKind.Internal);
        if (Activity is not null)
        {
            Activity.DisplayName = $"route {route}";
            Activity.SetTag("strata.route", route);
            Activity.SetTag("strata.route.call_id", CallId);
            if (node is not null)
            {
                Activity.SetTag("strata.thread_id", node.ThreadId);
                Activity.SetTag("strata.run_id", string.IsNullOrEmpty(node.RunId) ? null : node.RunId);
                Activity.SetTag("strata.node.path", node.NodePath);
                Activity.SetTag("strata.attempt", node.Attempt);
            }
        }
    }

    /// <summary>The call's activity; <see langword="null"/> when nothing listens to <c>Datalium.Strata</c>.</summary>
    public Activity? Activity { get; }

    /// <summary>The route's name.</summary>
    public string Route { get; }

    /// <summary>The route call's id (a UUID v7), on <see cref="ModelRouted"/> and the activity.</summary>
    public string CallId { get; }

    /// <summary>
    /// Starts recording a call through the route <paramref name="route"/>. <paramref name="node"/> is the calling
    /// node's context, which puts the call in that run's events; <see langword="null"/> outside a run.
    /// </summary>
    public static ModelRouteScope Start(NodeContext? node, string route)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(route);
        return new ModelRouteScope(node, route);
    }

    /// <summary>A health key's breaker changed state: <c>open</c>, <c>half_open</c> or <c>closed</c>.</summary>
    /// <remarks>A key's breaker belongs to no one call, so this is not on a scope.</remarks>
    public static void BreakerTransition(string healthKey, string state)
    {
        ArgumentNullException.ThrowIfNull(healthKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(state);
        StrataTelemetry.BreakerTransitions.Add(
            1,
            new TagList { { "strata.model.health_key", healthKey }, { "strata.model.breaker.state", state } });
    }

    /// <summary>A candidate started beside a slow one: <c>strata.route.hedges</c>.</summary>
    public void Hedged() => StrataTelemetry.RouteHedges.Add(1, new TagList { { "strata.route", Route } });

    /// <summary>
    /// The route moved past a candidate: <c>strata.route.failovers</c> with <c>strata.route.reason</c>
    /// <c>failed</c>, <c>refused</c>, <c>rejected</c>, <c>cooling_down</c>, <c>breaker_open</c> or <c>ineligible</c>.
    /// Any other outcome isn't a failover, and records nothing.
    /// </summary>
    public void FailedOver(ModelRouteOutcome reason)
    {
        var tag = reason switch
        {
            ModelRouteOutcome.Failed => "failed",
            ModelRouteOutcome.Refused => "refused",
            ModelRouteOutcome.Rejected => "rejected",
            ModelRouteOutcome.CoolingDown => "cooling_down",
            ModelRouteOutcome.BreakerOpen => "breaker_open",
            ModelRouteOutcome.Ineligible => "ineligible",
            _ => null,
        };
        if (tag is not null)
        {
            StrataTelemetry.RouteFailovers.Add(1, new TagList { { "strata.route", Route }, { "strata.route.reason", tag } });
        }
    }

    /// <summary>
    /// The call ended, after its last attempt: records <see cref="ModelRouted"/>, <c>strata.route.calls</c> and the
    /// activity's outcome. Only the first call records anything.
    /// </summary>
    /// <param name="outcome">
    /// <c>strata.outcome</c>: <c>answered</c>, <c>exhausted</c>, <c>ineligible</c>, <c>fatal</c>, <c>cancelled</c>, or
    /// <c>failed</c> for a streamed answer that broke, was refused or was rejected after it had gone out, or couldn't
    /// reach the node's stream.
    /// </param>
    /// <param name="errorType">The failure's class (<c>error.type</c>), when the call failed.</param>
    /// <param name="error">The failure's message, when the call failed.</param>
    public void Completed(
        string outcome,
        string? answered,
        string? model,
        IReadOnlyList<ModelRouteAttempt> attempts,
        int hedges,
        TimeSpan duration,
        string? errorType = null,
        string? error = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outcome);
        ArgumentNullException.ThrowIfNull(attempts);
        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            return;
        }

        StrataTelemetry.RouteCalls.Add(1, new TagList { { "strata.route", Route }, { "strata.outcome", outcome } });
        if (Activity is not null)
        {
            Activity.SetTag("strata.route.answered", answered);
            Activity.SetTag("strata.route.attempts", attempts.Count(a => a.CallId is not null));
            Activity.SetTag("strata.route.hedges", hedges);
            Activity.SetTag("strata.outcome", outcome);
            if (errorType is not null)
            {
                StrataTelemetry.Fail(Activity, error ?? errorType, errorType);
            }
        }

        if (_node?.Telemetry is { Observed: true } run)
        {
            run.Record(new ModelRouted(Route, CallId, answered, model, attempts, hedges, duration), _node.NodePath, _node.Attempt);
        }
    }

    /// <summary>Stops the activity.</summary>
    public void Dispose() => Activity?.Dispose();
}
