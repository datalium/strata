namespace Datalium.Strata.Cloud;

/// <summary>What went wrong talking to Strata Cloud (<c>design/30-Strata-Cloud-Store.md</c>, "Status mapping").</summary>
public enum StrataCloudErrorKind
{
    /// <summary><c>401</c>: no key, a malformed, revoked or expired one. Fix the credential; never retried.</summary>
    Unauthorized,

    /// <summary>
    /// <c>403 STRATA_CLOUD_API_ENTITLEMENT_EXCEEDED</c>: the save would start a new thread and the plan's
    /// limit of threads this month is reached. Refuse new executions; running ones carry on. Never retried.
    /// </summary>
    EntitlementExceeded,

    /// <summary>Any other <c>4xx</c> (<c>400</c>, <c>413</c>, <c>422</c>, …): the request will never succeed. Never retried.</summary>
    Rejected,

    /// <summary>
    /// Not now: a network error, a timeout, <c>429</c> or a <c>5xx</c>, after the call's retries (the
    /// decisions client makes one attempt). Asking again later may succeed.
    /// </summary>
    Unavailable,

    /// <summary>A success whose body isn't what the wire contract says.</summary>
    InvalidResponse,
}

/// <summary>
/// A request to Strata Cloud failed. Carries the HTTP status and the error envelope's <c>code</c>,
/// never anything else from the body, and never the key.
/// </summary>
public class StrataCloudException : Exception
{
    public StrataCloudException(
        StrataCloudErrorKind kind,
        string message,
        int? statusCode = null,
        string? code = null,
        TimeSpan? retryAfter = null,
        int attempts = 1,
        Exception? innerException = null)
        : base(message, innerException)
    {
        Kind = kind;
        StatusCode = statusCode;
        Code = code;
        RetryAfter = retryAfter;
        Attempts = attempts;
    }

    public StrataCloudErrorKind Kind { get; }

    /// <summary>The last response's HTTP status; <see langword="null"/> for a network error or a timeout.</summary>
    public int? StatusCode { get; }

    /// <summary>The error envelope's <c>code</c>, e.g. <c>STRATA_CLOUD_API_RATE_LIMITED</c>, when there was one.</summary>
    public string? Code { get; }

    /// <summary>The last response's <c>Retry-After</c>, when it had one.</summary>
    public TimeSpan? RetryAfter { get; }

    /// <summary>How many attempts the call made.</summary>
    public int Attempts { get; }

    /// <summary>Whether asking again later may succeed (<see cref="StrataCloudErrorKind.Unavailable"/>).</summary>
    public bool IsTransient => Kind == StrataCloudErrorKind.Unavailable;
}

/// <summary>
/// <c>403 STRATA_CLOUD_API_ENTITLEMENT_EXCEEDED</c> on a save: the account's plan has reached its limit
/// of new threads this month. Only a save that would start a thread is ever refused, so a host can catch
/// this around a new execution, refuse it, and let running ones carry on.
/// </summary>
public sealed class StrataCloudEntitlementException(string message, string? code, int attempts)
    : StrataCloudException(StrataCloudErrorKind.EntitlementExceeded, message, 403, code, retryAfter: null, attempts);
