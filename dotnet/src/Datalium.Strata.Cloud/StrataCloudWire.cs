namespace Datalium.Strata.Cloud;

/// <summary>
/// The names on Strata Cloud's SDK wire (contract v1, not public) that the
/// adapter depends on, in one place.
/// </summary>
public static class StrataCloudWire
{
    /// <summary>
    /// The response header on <c>GET /v1/sdk/checkpoints/latest</c> carrying the latest checkpoint's
    /// sequence (<c>design/30-Strata-Cloud-Store.md</c>, "The conditional save"). A Cloud without it
    /// leaves the store's loads without a sequence, and its saves unconditional.
    /// </summary>
    public const string SequenceHeader = "Strata-Sequence";

    /// <summary>
    /// The request header on <c>POST /v1/sdk/checkpoints</c> naming the sequence the thread's latest
    /// checkpoint must still have; a mismatch is <c>412</c>, with nothing stored.
    /// </summary>
    public const string IfSequenceHeader = "Strata-If-Sequence";

    /// <summary>
    /// The request header on <c>POST /v1/sdk/checkpoints</c> that makes it a save under a lease
    /// (<c>design/43-Cloud-Leases-And-Listing.md</c>): Cloud appends only if the token is still the thread's
    /// latest, and answers <c>423</c> with <see cref="LeaseLostCode"/> otherwise, nothing stored.
    /// </summary>
    public const string LeaseTokenHeader = "Strata-Lease-Token";

    /// <summary>
    /// The request header on <c>POST /v1/sdk/leases</c> that makes a retried acquisition answer the lease its
    /// first attempt took: one key per call, the same on each of its attempts.
    /// </summary>
    public const string IdempotencyKeyHeader = "Idempotency-Key";

    public const string LeaseLostCode = "STRATA_CLOUD_API_LEASE_LOST";

    public const string EntitlementExceededCode = "STRATA_CLOUD_API_ENTITLEMENT_EXCEEDED";

    public const string ThreadNotFoundCode = "STRATA_CLOUD_API_THREAD_NOT_FOUND";

    public const string PreconditionFailedCode = "STRATA_CLOUD_API_PRECONDITION_FAILED";

    internal const string SavePath = "v1/sdk/checkpoints";

    internal const string LoadPath = "v1/sdk/checkpoints/latest";

    internal const string DecisionsPath = "v1/sdk/decisions";

    internal const string LeasesPath = "v1/sdk/leases";

    internal const string ThreadsPath = "v1/sdk/threads";

    /// <summary><c>GET</c> on the save's path reads a thread's log.</summary>
    internal const string LogPath = "v1/sdk/checkpoints";

    internal const string SaveRoute = "checkpoints.save";

    internal const string LoadRoute = "checkpoints.load";

    internal const string DecisionsRoute = "decisions";

    internal const string AcquireRoute = "leases.acquire";

    internal const string RenewRoute = "leases.renew";

    internal const string ReleaseRoute = "leases.release";

    internal const string GetLeaseRoute = "leases.get";

    internal const string ThreadsRoute = "threads.list";

    internal const string LogRoute = "checkpoints.log";
}
