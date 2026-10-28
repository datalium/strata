using Datalium.Strata.Graph;

namespace Datalium.Strata.Cloud;

/// <summary>
/// Where Strata Cloud is and how to talk to it (<c>design/30-Strata-Cloud-Store.md</c>). Shared by
/// <see cref="StrataCloudCheckpointStore"/> and <see cref="StrataCloudDecisions"/>.
///
/// A class and not a record, so nothing prints <see cref="ApiKey"/>: keep the key in a secret store,
/// never in a graph's state or a run's metadata.
/// </summary>
public sealed class StrataCloudOptions
{
    /// <summary>
    /// The service's address, without the <c>/v1</c>: <c>https://api.strata.datalium.com</c>, or
    /// <c>http://localhost:9820</c> for the local stack. Routes are <c>/v1/sdk/…</c> under its path.
    /// </summary>
    public required Uri BaseUrl { get; init; }

    /// <summary>The API key (<c>strata_live_…</c>), sent as <c>Authorization: Bearer</c> on every request.</summary>
    public required string ApiKey { get; init; }

    /// <summary>
    /// How long a connection may take to open. Applies to the <see cref="HttpClient"/> the store or the
    /// decisions client creates; a caller passing its own client sets it there. Default 3 s.
    /// </summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>How long one attempt of a save or a load may take, response read included. Default 30 s.</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How saves and loads are retried. Default <see cref="StrataCloudRetryPolicy.Default"/> (contract §8).</summary>
    public StrataCloudRetryPolicy Retry { get; init; } = StrataCloudRetryPolicy.Default;

    /// <summary>A request body larger than this many bytes is sent gzipped. Default 64 KiB.</summary>
    public int GzipThresholdBytes { get; init; } = 64 * 1024;

    /// <summary>
    /// The clock for backoff waits, attempt timeouts and a <see cref="DecisionWorker{TState}"/>'s waits.
    /// Tests pass a <c>FakeTimeProvider</c>.
    /// </summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(BaseUrl);
        if (!BaseUrl.IsAbsoluteUri || (BaseUrl.Scheme != Uri.UriSchemeHttps && BaseUrl.Scheme != Uri.UriSchemeHttp))
        {
            throw new ArgumentException("BaseUrl must be an absolute http or https URL", nameof(BaseUrl));
        }

        if (string.IsNullOrWhiteSpace(ApiKey))
        {
            throw new ArgumentException("ApiKey is required", nameof(ApiKey));
        }

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ConnectTimeout, TimeSpan.Zero, nameof(ConnectTimeout));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(RequestTimeout, TimeSpan.Zero, nameof(RequestTimeout));
        ArgumentOutOfRangeException.ThrowIfNegative(GzipThresholdBytes, nameof(GzipThresholdBytes));
        ArgumentNullException.ThrowIfNull(Retry);
        ArgumentNullException.ThrowIfNull(TimeProvider);
    }
}

/// <summary>
/// How a save or a load is retried (<c>design/30-Strata-Cloud-Store.md</c>, "Retries"): on a network
/// error, a timeout, <c>408</c>, <c>429</c>, <c>500</c>, <c>502</c>, <c>503</c> and <c>504</c>, and for a
/// save <c>409</c>. Never on any other <c>4xx</c>.
/// </summary>
public sealed record StrataCloudRetryPolicy
{
    /// <summary>
    /// Contract §8: 4 attempts, backoff from 200 ms doubling to 5 s with full jitter, never shorter than
    /// <c>Retry-After</c>, which may be at most 30 s.
    /// </summary>
    public static StrataCloudRetryPolicy Default { get; } = new();

    /// <summary>How many attempts a call makes at most, the first included. At least 1.</summary>
    public int MaxAttempts { get; init; } = 4;

    /// <summary>The wait before each retry, before <c>Retry-After</c> is applied.</summary>
    public RetryBackoff Backoff { get; init; } =
        new(TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5), BackoffJitter.Full);

    /// <summary>
    /// The longest <c>Retry-After</c> a call waits for. A longer one fails the call at once with
    /// <see cref="StrataCloudErrorKind.Unavailable"/>, carrying it, rather than holding a save that long.
    /// </summary>
    public TimeSpan MaxRetryAfter { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The wait before the <paramref name="retry"/>-th retry (1-based), given the response's <c>Retry-After</c>.</summary>
    internal TimeSpan DelayBefore(int retry, TimeSpan? retryAfter, Random random)
    {
        var ceiling = Backoff.Initial;
        for (var i = 1; i < retry && ceiling < Backoff.Max; i++)
        {
            ceiling = ceiling > Backoff.Max / 2 ? Backoff.Max : ceiling * 2;
        }

        if (ceiling > Backoff.Max)
        {
            ceiling = Backoff.Max;
        }

        var computed = Backoff.Jitter switch
        {
            BackoffJitter.Full => ceiling * random.NextDouble(),
            BackoffJitter.Equal => (ceiling / 2) + ((ceiling / 2) * random.NextDouble()),
            _ => ceiling,
        };
        return retryAfter is { } after && after > computed ? after : computed;
    }
}
