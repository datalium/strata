using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Datalium.Strata.Cloud;

/// <summary>
/// The Cloud adapter's instrumentation (<c>design/30-Strata-Cloud-Store.md</c>, "Telemetry"): an
/// <see cref="ActivitySource"/> and a <see cref="Meter"/> named <see cref="Name"/>, beside the engine's
/// <c>Datalium.Strata</c>. Listen with <c>AddSource(StrataCloudTelemetry.Name)</c> /
/// <c>AddMeter(StrataCloudTelemetry.Name)</c>. Nothing recorded carries the key, a state, metadata
/// values or an override value.
/// </summary>
public static class StrataCloudTelemetry
{
    /// <summary>The name of the adapter's <see cref="ActivitySource"/> and <see cref="Meter"/>.</summary>
    public const string Name = "Datalium.Strata.Cloud";

    /// <summary>The span each HTTP attempt runs in.</summary>
    public const string RequestSpan = "strata.cloud.request";

    private static readonly string? Version = typeof(StrataCloudTelemetry).Assembly.GetName().Version?.ToString();

    internal static readonly ActivitySource Source = new(Name, Version);

    internal static readonly Meter Meter = new(Name, Version);

    internal static readonly Counter<long> Retries = Meter.CreateCounter<long>(
        "strata.cloud.retries", "{retry}", "Attempts that failed and were retried, by route and reason.");

    internal static readonly Histogram<double> RequestDuration = Meter.CreateHistogram<double>(
        "strata.cloud.request.duration", "s", "How long one HTTP attempt took, by route and status.");
}
