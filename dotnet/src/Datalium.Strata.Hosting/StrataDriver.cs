using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Datalium.Strata.Interruption;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Datalium.Strata.Hosting;

/// <summary>
/// The hosting package's own telemetry (<c>design/35-Hosting-Integration.md</c>, "Telemetry"): a meter named
/// <see cref="Name"/>. The runs a driver starts are ordinary runs, in the engine's telemetry.
/// </summary>
public static class StrataHostingTelemetry
{
    /// <summary>The meter's name: <c>Datalium.Strata.Hosting</c>.</summary>
    public const string Name = "Datalium.Strata.Hosting";

    private static readonly Meter Meter = new(Name);

    private static readonly Counter<long> Rounds = Meter.CreateCounter<long>(
        "strata.host.driver.rounds", "{round}", "Driver rounds, by driver and outcome.");

    private static readonly Counter<long> Threads = Meter.CreateCounter<long>(
        "strata.host.driver.threads", "{thread}", "Threads a driver's rounds woke, recovered or purged, by driver.");

    internal static void Round(string driver, bool ok, int threads)
    {
        Rounds.Add(
            1,
            new KeyValuePair<string, object?>("strata.driver", driver),
            new KeyValuePair<string, object?>("strata.outcome", ok ? "ok" : "error"));
        if (threads > 0)
        {
            Threads.Add(threads, new KeyValuePair<string, object?>("strata.driver", driver));
        }
    }
}

/// <summary>
/// How a driver stands (<c>design/35-Hosting-Integration.md</c>, "Health"): its rounds so far, and its last
/// failure while it keeps failing.
/// </summary>
/// <param name="Name">The driver: <c>wake</c>, <c>recovery</c>, <c>retention</c>, <c>memory-retention</c> or <c>decisions</c>, and for a graph's driver its state type.</param>
/// <param name="Rounds">Rounds finished, failed ones included.</param>
/// <param name="FailuresInARow">How many rounds have failed since the last one that worked. 0: healthy.</param>
/// <param name="LastRoundAt">When the last round finished.</param>
/// <param name="LastFailure">The type of the failure of the last failed round, while <paramref name="FailuresInARow"/> isn't 0.</param>
public sealed record DriverState(string Name, long Rounds, int FailuresInARow, DateTimeOffset? LastRoundAt, string? LastFailure);

/// <summary>The drivers of this host and how each stands.</summary>
public interface IStrataDriverStatus
{
    /// <summary>Every driver that has started, by name.</summary>
    IReadOnlyList<DriverState> Drivers { get; }
}

internal sealed class StrataDriverStatus : IStrataDriverStatus
{
    private readonly ConcurrentDictionary<string, DriverState> _drivers = new(StringComparer.Ordinal);

    public IReadOnlyList<DriverState> Drivers => [.. _drivers.Values.OrderBy(d => d.Name, StringComparer.Ordinal)];

    public void Started(string name) => _drivers.TryAdd(name, new DriverState(name, 0, 0, null, null));

    public void Succeeded(string name, DateTimeOffset at) =>
        _drivers.AddOrUpdate(
            name,
            _ => new DriverState(name, 1, 0, at, null),
            (_, was) => was with { Rounds = was.Rounds + 1, FailuresInARow = 0, LastRoundAt = at, LastFailure = null });

    public void Failed(string name, DateTimeOffset at, Exception failure) =>
        _drivers.AddOrUpdate(
            name,
            _ => new DriverState(name, 1, 1, at, failure.GetType().Name),
            (_, was) => was with
            {
                Rounds = was.Rounds + 1,
                FailuresInARow = was.FailuresInARow + 1,
                LastRoundAt = at,
                LastFailure = failure.GetType().Name,
            });
}

/// <summary>
/// A driver's loop (<c>design/35-Hosting-Integration.md</c>, "Drivers"): one round, a wait, again. A round that
/// throws is logged, counted and recorded, and the wait doubles for each failure in a row, up to
/// <see cref="MaxBackoff"/>; a driver never ends the host. When the host stops, the wait ends at once, and a
/// round in flight is told to start nothing more (the <c>stop</c> token of <see cref="RoundAsync"/>) and is
/// waited for: nothing is cancelled.
/// </summary>
internal abstract class StrataDriver(
    string kind, string name, StrataDriverStatus status, TimeProvider clock, ILoggerFactory loggers) : BackgroundService
{
    /// <summary>The longest a failing driver waits between rounds, unless its interval is longer.</summary>
    public static readonly TimeSpan MaxBackoff = TimeSpan.FromMinutes(5);

    private readonly ILogger _logger = loggers.CreateLogger($"{StrataHostingTelemetry.Name}.{kind}");

    /// <summary>The wait between two rounds that work.</summary>
    protected abstract TimeSpan Interval { get; }

    /// <summary>
    /// One round. Returns how many threads it woke, recovered or purged. When <paramref name="stop"/> fires it
    /// starts nothing more and returns once what it is doing has finished.
    /// </summary>
    protected abstract Task<int> RoundAsync(CancellationToken stop);

    /// <summary>The wait after <paramref name="failures"/> failed rounds in a row: the interval, doubled for each, capped.</summary>
    public static TimeSpan WaitAfter(TimeSpan interval, int failures)
    {
        if (failures <= 0)
        {
            return interval;
        }

        var cap = interval > MaxBackoff ? interval : MaxBackoff;
        var doubled = interval.TotalMilliseconds * Math.Pow(2, Math.Min(failures, 30));
        return doubled >= cap.TotalMilliseconds ? cap : TimeSpan.FromMilliseconds(doubled);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Off the host's start-up path: the first round may run a thread to its end.
        await Task.Yield();
        status.Started(name);
        _logger.LogInformation("Strata driver {Driver} started, every {Interval}", name, Interval);
        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var threads = await RoundAsync(stoppingToken).ConfigureAwait(false);
                failures = 0;
                status.Succeeded(name, clock.GetUtcNow());
                StrataHostingTelemetry.Round(kind, ok: true, threads);
                if (threads > 0)
                {
                    _logger.LogInformation("Strata driver {Driver} handled {Threads} thread(s)", name, threads);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                failures++;
                status.Failed(name, clock.GetUtcNow(), ex);
                StrataHostingTelemetry.Round(kind, ok: false, threads: 0);

                // The type, not the message: a store's failure can hold a connection string.
                _logger.LogWarning(
                    "Strata driver {Driver} failed a round ({Failure}); {Failures} in a row", name, ex.GetType().Name, failures);
            }

            try
            {
                await Task.Delay(WaitAfter(Interval, failures), clock, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("Strata driver {Driver} stopped", name);
    }
}

/// <summary>
/// The container's checkpoint store as the engine's drivers take it: one object with the store port and the
/// query port. Every member is forwarded, the lease members included (<c>design/32-Thread-Leases.md</c>).
/// </summary>
internal sealed class QueryableStore(ICheckpointStore store, ICheckpointQuery listing) : ICheckpointStore, ICheckpointQuery
{
    public void Save(Checkpoint checkpoint) => store.Save(checkpoint);

    public Checkpoint? Load(string threadId) => store.Load(threadId);

    public ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
        store.SaveAsync(checkpoint, cancellationToken);

    public ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
        store.LoadAsync(threadId, cancellationToken);

    public ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default) =>
        store.LoadLatestAsync(threadId, cancellationToken);

    public ValueTask<bool> SaveIfLatestAsync(
        Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default) =>
        store.SaveIfLatestAsync(checkpoint, expectedSequence, cancellationToken);

    public ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
        string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        store.TryAcquireLeaseAsync(threadId, owner, ttl, cancellationToken);

    public ValueTask<ThreadLease?> RenewLeaseAsync(
        ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default) =>
        store.RenewLeaseAsync(lease, ttl, cancellationToken);

    public ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default) =>
        store.ReleaseLeaseAsync(lease, cancellationToken);

    public ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default) =>
        store.GetLeaseAsync(threadId, cancellationToken);

    public ValueTask<bool> SaveUnderLeaseAsync(
        Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default) =>
        store.SaveUnderLeaseAsync(checkpoint, lease, expectedSequence, cancellationToken);

    public ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default) =>
        listing.ListThreadsAsync(query, cancellationToken);

    public ValueTask<CheckpointLogPage> GetLogAsync(
        string threadId,
        long? afterSequence = null,
        int limit = ThreadQuery.DefaultLimit,
        CancellationToken cancellationToken = default) =>
        listing.GetLogAsync(threadId, afterSequence, limit, cancellationToken);
}
