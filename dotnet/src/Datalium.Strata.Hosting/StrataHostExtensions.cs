using Datalium.Strata.Cloud;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace Datalium.Strata.Hosting;

/// <summary>
/// The names of Strata's activity sources and meters, for an application that wires OpenTelemetry by hand.
/// <see cref="StrataHostExtensions.AddInstrumentation"/> adds them all.
/// </summary>
public static class StrataInstrumentation
{
    /// <summary>The sources of Strata's spans: the engine's (run, node, attempt, branch, tool) and the Cloud client's.</summary>
    public static IReadOnlyList<string> ActivitySourceNames { get; } = [StrataTelemetry.Name, StrataCloudTelemetry.Name];

    /// <summary>The meters of Strata's metrics: the engine's, the stores', the Cloud client's and the drivers'.</summary>
    public static IReadOnlyList<string> MeterNames { get; } =
        [StrataTelemetry.Name, StorageTelemetry.Name, StrataCloudTelemetry.Name, StrataHostingTelemetry.Name];
}

/// <summary>Health checks and instrumentation (<c>design/35-Hosting-Integration.md</c>).</summary>
public static class StrataHostExtensions
{
    /// <summary>The name of the checkpoint store's health check.</summary>
    public const string CheckpointsCheck = "strata-checkpoints";

    /// <summary>The name of the drivers' health check.</summary>
    public const string DriversCheck = "strata-drivers";

    /// <summary>
    /// Adds two health checks. <see cref="CheckpointsCheck"/> loads a thread nobody has: an answer is
    /// healthy, a failure or no answer within <paramref name="timeout"/> (5 seconds by default) is unhealthy.
    /// <see cref="DriversCheck"/> is degraded while any driver's last round failed.
    /// </summary>
    public static IStrataBuilder AddHealthChecks(this IStrataBuilder builder, IEnumerable<string>? tags = null, TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var wait = timeout ?? TimeSpan.FromSeconds(5);
        if (wait <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "the health check's timeout must be more than zero");
        }

        var tagged = tags?.ToArray();
        builder.Services.AddHealthChecks()
            .Add(new HealthCheckRegistration(
                CheckpointsCheck,
                sp => new CheckpointStoreHealthCheck(sp.GetRequiredService<ICheckpointStore>(), wait),
                failureStatus: null,
                tagged))
            .Add(new HealthCheckRegistration(
                DriversCheck,
                sp => new DriverHealthCheck(sp.GetRequiredService<IStrataDriverStatus>()),
                failureStatus: HealthStatus.Degraded,
                tagged));
        return builder;
    }

    /// <summary>
    /// Adds every Strata activity source and meter (<see cref="StrataInstrumentation"/>) to the OpenTelemetry
    /// tracer and meter providers the application sets up. It creates no provider and exports nothing.
    /// </summary>
    public static IStrataBuilder AddInstrumentation(this IStrataBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.ConfigureOpenTelemetryTracerProvider(tracing => tracing.AddSource([.. StrataInstrumentation.ActivitySourceNames]));
        builder.Services.ConfigureOpenTelemetryMeterProvider(metrics => metrics.AddMeter([.. StrataInstrumentation.MeterNames]));
        return builder;
    }
}

/// <summary>
/// Whether the checkpoint store answers: a load of a thread nobody has. The failure's type is the
/// description, and the exception is attached for the application to render or not: a store's message can
/// hold a connection string.
/// </summary>
internal sealed class CheckpointStoreHealthCheck(ICheckpointStore store, TimeSpan timeout) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        limit.CancelAfter(timeout);
        try
        {
            // WaitAsync too: a store that ignores its token still gets no longer than the timeout.
            await store.LoadLatestAsync(StrataStoreExtensions.ProbeThreadId, limit.Token).AsTask()
                .WaitAsync(limit.Token)
                .ConfigureAwait(false);
            return HealthCheckResult.Healthy();
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new HealthCheckResult(context.Registration.FailureStatus, $"no answer within {timeout.TotalSeconds:0.###} s");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new HealthCheckResult(context.Registration.FailureStatus, ex.GetType().Name, ex);
        }
    }
}

/// <summary>Degraded while any driver's last round failed, naming them.</summary>
internal sealed class DriverHealthCheck(IStrataDriverStatus status) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var failing = status.Drivers.Where(driver => driver.FailuresInARow > 0).ToList();
        return Task.FromResult(failing.Count == 0
            ? HealthCheckResult.Healthy()
            : new HealthCheckResult(
                context.Registration.FailureStatus,
                "failing: " + string.Join(", ", failing.Select(driver => $"{driver.Name} ({driver.LastFailure}, {driver.FailuresInARow} in a row)"))));
    }
}
