using System.ClientModel;
using System.Globalization;
using System.Net;
using Datalium.Strata.Graph;

namespace Datalium.Strata.AI;

/// <summary>
/// A model call's failure as Strata reads it (<c>design/44-LLM-Integration.md</c>, "Faults"): worth retrying or not,
/// the provider's retry hint, and a low-cardinality class for <c>error.type</c>.
/// </summary>
/// <param name="IsTransient">A retry may succeed: 408, 409, 429, 5xx, a timeout, a lost connection, a broken stream.</param>
/// <param name="RetryAfter">How long the provider asked to wait (<c>retry-after-ms</c>, <c>Retry-After</c>).</param>
/// <param name="ErrorType">The HTTP status (<c>429</c>), <c>timeout</c>, <c>connection</c>, <c>stream_broken</c>, or an exception's type name.</param>
public sealed record ModelFault(bool IsTransient, TimeSpan? RetryAfter, string ErrorType);

/// <summary>
/// Reads a provider's own exception type, for providers the default classifier doesn't know (an SDK that throws
/// neither <see cref="HttpRequestException"/> nor <see cref="ClientResultException"/>).
/// </summary>
public interface IModelFaultClassifier
{
    /// <summary>The fault <paramref name="exception"/> is, or <see langword="null"/> to leave it to the default.</summary>
    ModelFault? Classify(Exception exception);
}

/// <summary>
/// A provider fault Strata understands, thrown by <see cref="RetryAfterHandler"/> or by an application's own client.
/// </summary>
public sealed class ModelCallException : Exception
{
    public ModelCallException(string message, bool isTransient, int? statusCode = null, TimeSpan? retryAfter = null, Exception? innerException = null)
        : base(message, innerException)
    {
        IsTransient = isTransient;
        StatusCode = statusCode;
        RetryAfter = retryAfter;
    }

    /// <summary>A retry may succeed.</summary>
    public bool IsTransient { get; }

    /// <summary>The HTTP status the provider answered, when it answered.</summary>
    public int? StatusCode { get; }

    /// <summary>How long the provider asked to wait.</summary>
    public TimeSpan? RetryAfter { get; }
}

/// <summary>
/// How a model call's exception becomes a node failure (<c>design/44-LLM-Integration.md</c>, "Faults"): transient with
/// the provider's hint, so a <c>RecoveryPolicy</c> with a backoff retries it after that long (<c>design/25</c>), or fatal.
/// </summary>
public static class ModelFaults
{
    /// <summary>
    /// Whether an HTTP status is worth retrying: 408, 409, 429 and every 5xx (Anthropic's 529, overloaded,
    /// included). Every other status is the request's fault, and a retry would get the same answer.
    /// </summary>
    public static bool IsTransientStatus(int status) => status is 408 or 409 or 429 or >= 500 and <= 599;

    /// <summary>
    /// Reads <paramref name="exception"/>: a model route's own failure (<see cref="ModelRouteException"/>: its kind, hint
    /// and error type), <see cref="ModelCallException"/>, then <paramref name="classifier"/>, then
    /// <see cref="HttpRequestException"/>, <see cref="ClientResultException"/> (what the OpenAI and Azure SDKs throw),
    /// <see cref="TimeoutException"/> and an <see cref="OperationCanceledException"/> the caller didn't ask for (an
    /// HTTP client's timeout). Anything else is fatal, classed by its type.
    /// </summary>
    public static ModelFault Classify(Exception exception, IModelFaultClassifier? classifier = null)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (exception is ModelRouteException route)
        {
            return new ModelFault(route.IsTransient, route.RetryAfter, route.ErrorType);
        }

        if (exception is ModelCallException call)
        {
            return new ModelFault(
                call.IsTransient,
                call.RetryAfter,
                call.StatusCode?.ToString(CultureInfo.InvariantCulture) ?? (call.IsTransient ? "connection" : nameof(ModelCallException)));
        }

        if (classifier?.Classify(exception) is { } fault)
        {
            return fault;
        }

        return exception switch
        {
            HttpRequestException { StatusCode: { } status } => Status((int)status, null),
            HttpRequestException => new ModelFault(true, null, "connection"),
            ClientResultException { Status: 0 } => new ModelFault(true, null, "connection"),
            ClientResultException result => Status(result.Status, RetryAfter.From(result)),
            TimeoutException or OperationCanceledException => new ModelFault(true, null, "timeout"),
            IOException => new ModelFault(true, null, "connection"),
            _ => new ModelFault(false, null, exception.GetType().FullName ?? exception.GetType().Name),
        };
    }

    /// <summary>
    /// The node failure <paramref name="fault"/> makes: <see cref="NodeException.Transient(string, TimeSpan?, Exception?)"/>
    /// with its hint, or <see cref="NodeException.Fatal"/>, wrapping <paramref name="exception"/>.
    /// </summary>
    public static NodeException ToNodeException(ModelFault fault, Exception exception)
    {
        ArgumentNullException.ThrowIfNull(fault);
        ArgumentNullException.ThrowIfNull(exception);
        var message = $"model call failed ({fault.ErrorType}): {exception.Message}";
        return fault.IsTransient
            ? NodeException.Transient(message, fault.RetryAfter, exception)
            : NodeException.Fatal(message, exception);
    }

    private static ModelFault Status(int status, TimeSpan? retryAfter) =>
        new(IsTransientStatus(status), retryAfter, status.ToString(CultureInfo.InvariantCulture));
}

/// <summary>
/// A provider's retry hint (<c>design/44-LLM-Integration.md</c>): <c>retry-after-ms</c> (OpenAI's), else
/// <c>Retry-After</c> as seconds or an HTTP date.
/// </summary>
public static class RetryAfter
{
    /// <summary>
    /// The wait the headers ask for, or <see langword="null"/> when they ask for none or can't be read. A date in the
    /// past is no wait at all (<see cref="TimeSpan.Zero"/>).
    /// </summary>
    public static TimeSpan? Parse(string? retryAfterMs, string? retryAfter, DateTimeOffset now)
    {
        if (double.TryParse(retryAfterMs, NumberStyles.Float, CultureInfo.InvariantCulture, out var ms) && ms >= 0)
        {
            return TimeSpan.FromMilliseconds(ms);
        }

        if (string.IsNullOrWhiteSpace(retryAfter))
        {
            return null;
        }

        if (double.TryParse(retryAfter, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds))
        {
            return seconds >= 0 ? TimeSpan.FromSeconds(seconds) : null;
        }

        if (DateTimeOffset.TryParse(retryAfter, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var at))
        {
            var wait = at - now;
            return wait > TimeSpan.Zero ? wait : TimeSpan.Zero;
        }

        return null;
    }

    /// <summary>The hint on an HTTP response.</summary>
    public static TimeSpan? From(HttpResponseMessage response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return Parse(Header(response, "retry-after-ms"), Header(response, "Retry-After"), DateTimeOffset.UtcNow);
    }

    /// <summary>The hint on the response a <see cref="ClientResultException"/> carries, if it carries one.</summary>
    public static TimeSpan? From(ClientResultException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        var headers = exception.GetRawResponse()?.Headers;
        if (headers is null)
        {
            return null;
        }

        headers.TryGetValue("retry-after-ms", out var ms);
        headers.TryGetValue("Retry-After", out var after);
        return Parse(ms, after, DateTimeOffset.UtcNow);
    }

    private static string? Header(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) ? values.FirstOrDefault() : null;
}

/// <summary>
/// A handler for an <see cref="HttpClient"/>-based provider (<c>design/44-LLM-Integration.md</c>): a response whose
/// status is worth retrying (<see cref="ModelFaults.IsTransientStatus"/>) becomes a <see cref="ModelCallException"/>
/// carrying the status and the provider's retry hint, which the provider's SDK would otherwise drop. Every other
/// response passes through for the SDK to read.
/// </summary>
/// <remarks>Turn the SDK's own retries off, so Strata's policy, which sees the hint, does the retrying.</remarks>
public sealed class RetryAfterHandler : DelegatingHandler
{
    private const int MessageLimit = 512;

    public RetryAfterHandler()
    {
    }

    public RetryAfterHandler(HttpMessageHandler innerHandler)
        : base(innerHandler)
    {
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        var status = (int)response.StatusCode;
        if (!ModelFaults.IsTransientStatus(status))
        {
            return response;
        }

        using (response)
        {
            var retryAfter = RetryAfter.From(response);
            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            if (body.Length > MessageLimit)
            {
                body = body[..MessageLimit];
            }

            var reason = response.ReasonPhrase ?? ((HttpStatusCode)status).ToString();
            throw new ModelCallException(
                string.IsNullOrWhiteSpace(body) ? $"{status} {reason}" : $"{status} {reason}: {body}",
                isTransient: true,
                statusCode: status,
                retryAfter: retryAfter);
        }
    }
}
