using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Datalium.Strata.Graph;
using Datalium.Strata.Observability;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>
/// One candidate of a <see cref="ModelRoute"/> (<c>design/46-Model-Routes.md</c>). Mirrors the Rust <c>ModelCandidate</c>.
/// </summary>
public sealed class ModelCandidate
{
    /// <param name="name">The candidate's name, unique within its route: in spans, events and messages.</param>
    /// <param name="client">The client that answers for it.</param>
    /// <param name="model">The model asked for in its place of the request's, when it names one; blank is refused, as in Rust.</param>
    public ModelCandidate(string name, IChatClient client, string? model = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(client);
        if (model is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(model);
        }

        Name = name;
        Client = client;
        Model = model;
    }

    /// <summary>The candidate's name.</summary>
    public string Name { get; }

    /// <summary>The client that answers for it.</summary>
    public IChatClient Client { get; }

    /// <summary>The model it asks for in place of the request's; <see langword="null"/> to ask for the request's.</summary>
    public string? Model { get; }

    /// <summary>
    /// The key its health is kept under (<see cref="ModelHealth"/>): the client's provider name
    /// (<see cref="ChatClientMetadata.ProviderName"/>) when not given, as an outage or a rate limit takes every model of
    /// a provider, else the candidate's name. Name one per model or per region to keep them apart.
    /// </summary>
    public string? HealthKey { get; init; }

    /// <summary>
    /// The application's rule for this candidate (tenant, budget tier, tools, context size, …), asked when its turn
    /// comes: <see langword="null"/> to ask it, or why not, which skips it as <see cref="ModelRouteOutcome.Ineligible"/>.
    /// It gets the request's messages (for a context-size rule), its options (an empty set when the request had none)
    /// and the calling node's context (<see langword="null"/> outside a node), as Rust's gets the whole request.
    /// </summary>
    public Func<IReadOnlyList<ChatMessage>, ChatOptions, NodeContext?, string?>? Eligible { get; init; }
}

/// <summary>A <see cref="ModelRoute"/>'s options. Mirrors the Rust <c>ModelRoute</c> builders.</summary>
public sealed record ModelRouteOptions
{
    /// <summary>The route's name: in its spans (<c>route &lt;name&gt;</c>), events, metrics and messages.</summary>
    public required string Name { get; init; }

    /// <summary>The candidates, in the order they are asked: at least one, names unique.</summary>
    public required IReadOnlyList<ModelCandidate> Candidates { get; init; }

    /// <summary>
    /// Judges an answer: <see langword="null"/> to accept it, or a reason, which moves the route on
    /// (<see cref="ModelRouteOutcome.Rejected"/>). <see langword="null"/> accepts everything.
    /// </summary>
    public Func<ChatResponse, string?>? Accept { get; init; }

    /// <summary>
    /// How long an attempt may go without an answer (or a stream without its first update) before the next candidate
    /// starts beside it; <see langword="null"/> (the default) for no hedging. Above zero.
    /// </summary>
    public TimeSpan? HedgeAfter { get; init; }

    /// <summary>The health it reads and keeps; a route-private one with no breaker when not given.</summary>
    public ModelHealth? Health { get; init; }

    /// <summary>Reads a provider's own exceptions before the default does (<see cref="ModelFaults.Classify"/>).</summary>
    public IModelFaultClassifier? FaultClassifier { get; init; }

    /// <summary>
    /// Called once per call through the route, inside a run or not, with what <see cref="ModelRouted"/> carries, before
    /// the call returns: for a caller outside a run (a ledger). It must not block; what it throws is ignored.
    /// </summary>
    public Action<ModelRouteResult>? OnRouted { get; init; }
}

/// <summary>A call through a route, as <see cref="ModelRouteOptions.OnRouted"/> gets it: what <see cref="ModelRouted"/> carries.</summary>
/// <param name="Answered">The candidate whose answer is the call's, <see langword="null"/> when the route failed.</param>
/// <param name="Model">The model that answered, <see langword="null"/> when none did.</param>
/// <param name="Attempts">Every candidate, once each, in the route's order.</param>
/// <param name="Hedges">How many candidates started because the one before was slow.</param>
public sealed record ModelRouteResult(
    string Route, string? Answered, string? Model, IReadOnlyList<ModelRouteAttempt> Attempts, int Hedges, TimeSpan Duration);

/// <summary>
/// A model route's own failure (<c>design/46-Model-Routes.md</c>), which <see cref="ModelFaults.Classify"/> reads first:
/// <c>route_exhausted</c> (transient, with the shortest wait until a candidate is free), <c>route_ineligible</c> (fatal:
/// no candidate could take the request), or <c>rejected</c> / <c>refused</c> (transient: a streamed answer that had
/// already gone out). A candidate's fatal fault is not wrapped: it is thrown as it is.
/// </summary>
public sealed class ModelRouteException : Exception
{
    public ModelRouteException(
        string route, string message, bool isTransient, TimeSpan? retryAfter, string errorType, IReadOnlyList<ModelRouteAttempt> attempts)
        : base(message)
    {
        ArgumentNullException.ThrowIfNull(route);
        ArgumentException.ThrowIfNullOrWhiteSpace(errorType);
        ArgumentNullException.ThrowIfNull(attempts);
        Route = route;
        IsTransient = isTransient;
        RetryAfter = retryAfter;
        ErrorType = errorType;
        Attempts = attempts;
    }

    /// <summary>The route's name.</summary>
    public string Route { get; }

    /// <summary>A retry may succeed.</summary>
    public bool IsTransient { get; }

    /// <summary>The shortest wait until some candidate can be asked again; <see langword="null"/> when one can be now.</summary>
    public TimeSpan? RetryAfter { get; }

    /// <summary><c>route_exhausted</c>, <c>route_ineligible</c>, <c>rejected</c> or <c>refused</c>.</summary>
    public string ErrorType { get; }

    /// <summary>Every candidate's outcome, in the route's order.</summary>
    public IReadOnlyList<ModelRouteAttempt> Attempts { get; }
}

/// <summary>
/// A model route (<c>design/46-Model-Routes.md</c>): a chat client over ordered candidates that fails over and,
/// optionally, hedges. Anything that takes an <see cref="IChatClient"/> takes a route; through <see cref="StrataChat"/>
/// (and so the model node and <see cref="StrataAgent{TOutput}"/>) each attempt is a model call of its own in the run.
/// Mirrors the Rust <c>ModelRoute</c>.
/// </summary>
/// <remarks>
/// <para>
/// A call asks the first available candidate, with the candidate's model in place of the request's. A transient fault,
/// a refusal (finish reason <c>content_filter</c>) or an answer <see cref="ModelRouteOptions.Accept"/> rejects moves it
/// on; a fatal fault stops it, thrown as it is. A candidate is skipped when its eligibility check gives a reason, or its
/// health key is cooling down or its breaker open. With nothing left, the call throws a <see cref="ModelRouteException"/>.
/// No candidate is asked twice in one call: the node's <c>RecoveryPolicy</c> retries the whole route.
/// </para>
/// <para>
/// With <see cref="ModelRouteOptions.HedgeAfter"/>, an attempt slow to answer (or to give a stream's first update) gets
/// the next candidate started beside it; the first accepted answer, or the first stream to give an update, wins, and the
/// others are cancelled. A stream fails over only until its first update: after that the route has committed, and a
/// broken stream, a refusal or a rejected answer fails the call.
/// </para>
/// </remarks>
public sealed class ModelRoute : IChatClient
{
    private readonly ModelRouteOptions _options;

    public ModelRoute(ModelRouteOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Name, nameof(options));
        ArgumentNullException.ThrowIfNull(options.Candidates, nameof(options));
        if (options.Candidates.Count == 0)
        {
            throw new ArgumentException($"route '{options.Name}' has no candidates", nameof(options));
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var candidate in options.Candidates)
        {
            ArgumentNullException.ThrowIfNull(candidate, nameof(options));
            if (!names.Add(candidate.Name))
            {
                throw new ArgumentException($"route '{options.Name}' has two candidates named '{candidate.Name}'", nameof(options));
            }
        }

        if (options.HedgeAfter is { } hedge)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(hedge, TimeSpan.Zero, nameof(options));
        }

        _options = options;
        Health = options.Health ?? new ModelHealth();
        Candidates = [.. options.Candidates.Select(c =>
        {
            var metadata = c.Client.GetService<ChatClientMetadata>();
            return new Resolved(c, c.HealthKey ?? metadata?.ProviderName ?? c.Name, metadata?.ProviderName, metadata?.DefaultModelId);
        })];
    }

    /// <summary>The route's name.</summary>
    public string Name => _options.Name;

    /// <summary>The options it was made with.</summary>
    public ModelRouteOptions Options => _options;

    /// <summary>The health it reads and keeps.</summary>
    public ModelHealth Health { get; }

    /// <summary>The candidates with what the route resolved for them once.</summary>
    internal IReadOnlyList<Resolved> Candidates { get; }

    /// <summary>
    /// Routes the request. Outside a node: the attempts are model calls with spans and metrics but no run events (use
    /// <see cref="StrataChat"/> from a node to have them).
    /// </summary>
    /// <exception cref="ModelRouteException">No candidate answered, or none could take the request.</exception>
    /// <remarks>A candidate's fatal fault is thrown as the candidate threw it.</remarks>
    public async Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        await using var call = new RouteCall(this, messages, options, null, null, _options.FaultClassifier, streaming: false, cancellationToken);
        var step = await call.StartAsync().ConfigureAwait(false);
        if (step.Kind != RouteStepKind.Answer)
        {
            Throw(step, cancellationToken);
        }

        return step.Response!;
    }

    /// <summary>
    /// Routes the request as a stream: the first candidate to give an update wins, and only its updates come out. A
    /// failure after that is thrown as it is; a refusal or a rejected answer at the end is a transient
    /// <see cref="ModelRouteException"/> (<c>refused</c>, <c>rejected</c>), since its updates have gone out.
    /// </summary>
    public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        await using var call = new RouteCall(this, messages, options, null, null, _options.FaultClassifier, streaming: true, cancellationToken);
        var step = await call.StartAsync().ConfigureAwait(false);
        while (step.Kind == RouteStepKind.Update)
        {
            yield return step.Update!;
            step = await call.NextAsync().ConfigureAwait(false);
        }

        if (step.Kind != RouteStepKind.Answer)
        {
            Throw(step, cancellationToken);
        }

        // A winner whose stream ended before any update: whatever its answer holds.
        if (call.Streamed == 0)
        {
            foreach (var update in step.Response!.ToChatResponseUpdates())
            {
                yield return update;
            }
        }
    }

    /// <summary>The route itself, for <see cref="ModelRoute"/> or any type it is; nothing else.</summary>
    public object? GetService(Type serviceType, object? serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(serviceType);
        return serviceKey is null && serviceType.IsInstanceOfType(this) ? this : null;
    }

    /// <summary>Does nothing: the candidates' clients are the application's.</summary>
    public void Dispose()
    {
    }

    /// <summary>
    /// The route from a node (<see cref="StrataChat"/>): each attempt is a model call in the node's run, the winner's
    /// text goes to the node's stream when it has one, and a failure is a <see cref="NodeException"/>.
    /// </summary>
    internal async Task<ChatResponse> CompleteAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options,
        NodeContext? context,
        StrataChatOptions? strata,
        CancellationToken cancellationToken)
    {
        var stream = context?.Stream;
        await using var call = new RouteCall(
            this,
            messages,
            options,
            context,
            strata?.Agent,
            _options.FaultClassifier ?? strata?.FaultClassifier,
            streaming: stream is not null,
            cancellationToken);
        var step = await call.StartAsync().ConfigureAwait(false);
        while (step.Kind == RouteStepKind.Update)
        {
            var text = step.Update!.Text;
            if (!string.IsNullOrEmpty(text))
            {
                try
                {
                    await stream!.EmitTokenAsync(text, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // The node's stream is gone (a closed channel): the call can't deliver its answer, as Rust's
                    // stream_closed. A cancellation goes the way every cancellation does.
                    step = await call.StreamClosedAsync(ex).ConfigureAwait(false);
                    break;
                }
            }

            step = await call.NextAsync().ConfigureAwait(false);
        }

        switch (step.Kind)
        {
            case RouteStepKind.Answer:
                return step.Response!;
            case RouteStepKind.Cancelled:
                throw new OperationCanceledException(cancellationToken);
            default:
                switch (step.Error)
                {
                    case NodeException:
                        // A client that already speaks Strata's language: its failure stands as it is.
                        ExceptionDispatchInfo.Capture(step.Error).Throw();
                        break;
                    case ModelRouteException route:
                        // Worded as any model call's failure, as Rust's NodeError::from(ChatError) words it.
                        throw ModelFaults.ToNodeException(ModelFaults.Classify(route), route);
                }

                throw ModelFaults.ToNodeException(step.Fault!, step.Error!);
        }
    }

    [DoesNotReturn]
    private static void Throw(RouteStep step, CancellationToken cancellationToken)
    {
        if (step.Kind == RouteStepKind.Failure)
        {
            ExceptionDispatchInfo.Capture(step.Error!).Throw();
        }

        throw new OperationCanceledException(cancellationToken);
    }

    /// <summary>A candidate with its health key, provider and default model, read from its client once.</summary>
    internal sealed record Resolved(ModelCandidate Candidate, string HealthKey, string? Provider, string? DefaultModel);
}
