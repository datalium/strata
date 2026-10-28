using System.Diagnostics;
using System.Globalization;
using Datalium.Strata.Graph;
using Datalium.Strata.Observability;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>What a <see cref="RouteCall"/> step gave.</summary>
internal enum RouteStepKind
{
    /// <summary>The winner's next stream update.</summary>
    Update,

    /// <summary>The call's answer: the call is over.</summary>
    Answer,

    /// <summary>The call failed: <see cref="RouteStep.Error"/> is the route's own exception or a candidate's.</summary>
    Failure,

    /// <summary>The caller's token stopped the call.</summary>
    Cancelled,
}

/// <param name="Fault">How <see cref="Error"/> fails a node.</param>
internal sealed record RouteStep(
    RouteStepKind Kind, ChatResponseUpdate? Update = null, ChatResponse? Response = null, Exception? Error = null, ModelFault? Fault = null);

/// <summary>
/// One call through a <see cref="ModelRoute"/> (<c>design/46-Model-Routes.md</c>, "Semantics"): it chooses the
/// candidates in turn, runs each attempt as a model call of its own, hedges, judges the answers, keeps the health, and
/// records <see cref="ModelRouted"/> once, whichever way the call ends. <see cref="StartAsync"/> runs until there is an
/// answer, a failure or (streaming) a winner's first update; <see cref="NextAsync"/> gives the winner's later updates.
/// Disposing a call that hasn't ended stops it as cancelled.
/// </summary>
internal sealed class RouteCall : IAsyncDisposable
{
    private readonly ModelRoute _route;
    private readonly IReadOnlyList<ModelRoute.Resolved> _candidates;
    private readonly List<ChatMessage> _messages;
    private readonly ChatOptions? _options;
    private readonly NodeContext? _node;
    private readonly string? _agent;
    private readonly IModelFaultClassifier? _classifier;
    private readonly bool _streaming;
    private readonly CancellationToken _cancellationToken;
    private readonly ModelHealth _health;
    private readonly TimeProvider _time;
    private readonly long _started;
    private readonly ModelRouteScope _scope;
    private readonly ModelRouteOutcome?[] _outcomes;
    private readonly string?[] _reasons;
    private readonly string?[] _callIds;
    private readonly List<Attempt> _running = [];
    private readonly List<Attempt> _all = [];
    private readonly List<ChatResponseUpdate> _updates = [];
    private int _next;
    private int _hedges;
    private Attempt? _winner;
    private bool _finished;

    public RouteCall(
        ModelRoute route,
        IEnumerable<ChatMessage> messages,
        ChatOptions? options,
        NodeContext? node,
        string? agent,
        IModelFaultClassifier? classifier,
        bool streaming,
        CancellationToken cancellationToken)
    {
        _route = route;
        _candidates = route.Candidates;
        // Every attempt sends the same messages: read them once.
        _messages = [.. messages];
        _options = options;
        _node = node;
        _agent = agent;
        _classifier = classifier;
        _streaming = streaming;
        _cancellationToken = cancellationToken;
        _health = route.Health;
        _time = _health.TimeProvider;
        _started = _time.GetTimestamp();
        _outcomes = new ModelRouteOutcome?[_candidates.Count];
        _reasons = new string?[_candidates.Count];
        _callIds = new string?[_candidates.Count];
        _scope = ModelRouteScope.Start(node, route.Name);
    }

    /// <summary>How many of the winner's updates have come out.</summary>
    public int Streamed => _updates.Count;

    /// <summary>Runs the route until it has an answer, a failure, or (streaming) a winner's first update.</summary>
    public async Task<RouteStep> StartAsync()
    {
        var hedgeAfter = _route.Options.HedgeAfter;
        CancellationTokenSource? timerToken = null;
        Task? timer = null;
        var deadline = DateTimeOffset.MaxValue;

        void Restart()
        {
            // The hedge timer runs from the latest attempt's start.
            deadline = hedgeAfter is { } after ? _time.GetUtcNow() + after : DateTimeOffset.MaxValue;
            DropTimer();
        }

        void DropTimer()
        {
            timerToken?.Cancel();
            timerToken?.Dispose();
            timerToken = null;
            timer = null;
        }

        try
        {
            while (true)
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    return await CancelledAsync().ConfigureAwait(false);
                }

                if (_running.Count == 0)
                {
                    // Nothing in flight: the next available candidate starts at once (failover).
                    if (!StartNext())
                    {
                        return Exhausted();
                    }

                    Restart();
                }

                var waits = new List<Task>(_running.Count + 1);
                waits.AddRange(_running.Select(a => (Task)a.Task));
                if (hedgeAfter is not null && _next < _candidates.Count)
                {
                    if (timer is null)
                    {
                        timerToken = CancellationTokenSource.CreateLinkedTokenSource(_cancellationToken);
                        var left = deadline - _time.GetUtcNow();
                        timer = Task.Delay(left > TimeSpan.Zero ? left : TimeSpan.Zero, _time, timerToken.Token);
                    }

                    waits.Add(timer);
                }

                var done = await Task.WhenAny(waits).ConfigureAwait(false);
                if (done == timer)
                {
                    DropTimer();
                    if (!_cancellationToken.IsCancellationRequested && StartNext())
                    {
                        _hedges++;
                        _scope.Hedged();
                        Restart();
                    }

                    continue;
                }

                var attempt = _running.First(a => a.Task == done);
                _running.Remove(attempt);
                var end = await EndOf(attempt).ConfigureAwait(false);
                if (_cancellationToken.IsCancellationRequested)
                {
                    await AbandonAsync(attempt, end, ModelRouteOutcome.Cancelled).ConfigureAwait(false);
                    return await CancelledAsync().ConfigureAwait(false);
                }

                if (await JudgeAsync(attempt, end).ConfigureAwait(false) is { } step)
                {
                    return step;
                }

                // An attempt that ended without winning while others run: the hedge timer keeps going.
            }
        }
        finally
        {
            DropTimer();
        }
    }

    /// <summary>The winner's next update, or how its stream ended: judged, and the call over.</summary>
    public async Task<RouteStep> NextAsync()
    {
        var winner = _winner ?? throw new InvalidOperationException("the route has no stream to continue");
        var candidate = _candidates[winner.Index];
        bool more;
        try
        {
            more = await winner.Stream!.MoveNextAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (winner.Token.IsCancellationRequested)
        {
            await DisposeQuietlyAsync(winner.Stream).ConfigureAwait(false);
            winner.Scope!.Cancelled();
            winner.Scope.Dispose();
            _health.Released(candidate.HealthKey, winner.Test);
            Settle(winner.Index, ModelRouteOutcome.Cancelled, null);
            return Finish(new RouteStep(RouteStepKind.Cancelled), "cancelled", null, null, "cancelled", "cancelled");
        }
        catch (Exception ex)
        {
            // The route has committed: a stream that breaks now fails the call, and counts against the key.
            await DisposeQuietlyAsync(winner.Stream).ConfigureAwait(false);
            var fault = Classify(ex);
            winner.Scope!.Failed("stream_broken", ex.Message);
            winner.Scope.Dispose();
            _health.Failed(candidate.HealthKey, fault.RetryAfter, winner.Test);
            Settle(winner.Index, ModelRouteOutcome.Failed, "stream_broken");
            return Finish(
                new RouteStep(RouteStepKind.Failure, Error: ex, Fault: fault with { IsTransient = true, ErrorType = "stream_broken" }),
                "failed",
                null,
                null,
                "stream_broken",
                ex.Message);
        }

        if (more)
        {
            var update = winner.Stream.Current;
            _updates.Add(update);
            return new RouteStep(RouteStepKind.Update, update);
        }

        await DisposeQuietlyAsync(winner.Stream).ConfigureAwait(false);
        var response = _updates.ToChatResponse();
        winner.Scope!.Succeeded(StrataChat.Usage(response.Usage), response.FinishReason?.Value, response.ModelId, response.ResponseId);
        winner.Scope.Dispose();

        // Its tokens have gone out, so an answer the route would have moved on from fails the call instead.
        string? refusal;
        if (response.FinishReason == ChatFinishReason.ContentFilter)
        {
            refusal = $"route '{_route.Name}': {candidate.Candidate.Name}'s streamed answer was refused";
            Settle(winner.Index, ModelRouteOutcome.Refused, null);
            return Committed(winner, refusal, "refused");
        }

        try
        {
            refusal = _route.Options.Accept?.Invoke(response);
        }
        catch (Exception ex)
        {
            _health.Released(candidate.HealthKey, winner.Test);
            Settle(winner.Index, ModelRouteOutcome.Rejected, ex.Message);
            return Finish(new RouteStep(RouteStepKind.Failure, Error: ex, Fault: Classify(ex)), "fatal", null, null, "accept", ex.Message);
        }

        if (refusal is not null)
        {
            Settle(winner.Index, ModelRouteOutcome.Rejected, refusal);
            return Committed(winner, $"route '{_route.Name}': {candidate.Candidate.Name}'s streamed answer was rejected ({refusal})", "rejected");
        }

        _health.Succeeded(candidate.HealthKey, winner.Test);
        Settle(winner.Index, ModelRouteOutcome.Answered, null);
        return Finish(
            new RouteStep(RouteStepKind.Answer, Response: response), "answered", candidate.Candidate.Name, ModelOf(response, candidate), null, null);
    }

    /// <summary>
    /// The winner's tokens couldn't reach the node's stream: the call fails transiently (<c>stream_closed</c>), the winner
    /// is stopped and recorded <see cref="ModelRouteOutcome.Failed"/>, and its key's health is untouched (the provider
    /// did nothing wrong).
    /// </summary>
    public async Task<RouteStep> StreamClosedAsync(Exception exception)
    {
        var winner = _winner ?? throw new InvalidOperationException("the route has no stream to close");
        await winner.Cancellation.CancelAsync().ConfigureAwait(false);
        await DisposeQuietlyAsync(winner.Stream).ConfigureAwait(false);
        winner.Scope!.Failed("stream_closed", exception.Message);
        winner.Scope.Dispose();
        _health.Released(_candidates[winner.Index].HealthKey, winner.Test);
        Settle(winner.Index, ModelRouteOutcome.Failed, "stream_closed");
        return Finish(
            new RouteStep(RouteStepKind.Failure, Error: exception, Fault: new ModelFault(true, null, "stream_closed")),
            "failed",
            null,
            null,
            "stream_closed",
            exception.Message);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_finished)
        {
            // Abandoned before it ended (the caller stopped reading the stream, or something threw): whatever was in
            // flight was cancelled.
            if (_winner is { Stream: not null } winner)
            {
                await winner.Cancellation.CancelAsync().ConfigureAwait(false);
                await DisposeQuietlyAsync(winner.Stream).ConfigureAwait(false);
                winner.Scope?.Cancelled();
                winner.Scope?.Dispose();
                _health.Released(_candidates[winner.Index].HealthKey, winner.Test);
                Settle(winner.Index, ModelRouteOutcome.Cancelled, null);
            }

            await StopAsync(ModelRouteOutcome.Cancelled).ConfigureAwait(false);
            Finish(new RouteStep(RouteStepKind.Cancelled), "cancelled", null, null, "cancelled", "cancelled");
        }

        foreach (var attempt in _all.Where(a => !a.Reaped))
        {
            attempt.Cancellation.Dispose();
        }

        _scope.Dispose();
    }

    /// <summary>
    /// Starts the next candidate that may be asked, skipping (and recording) the ones that may not; false when none is
    /// left. A candidate's state is decided now, when its turn comes.
    /// </summary>
    private bool StartNext()
    {
        while (_next < _candidates.Count)
        {
            var index = _next++;
            var candidate = _candidates[index];
            if (candidate.Candidate.Eligible?.Invoke(_messages, _options ?? new ChatOptions(), _node) is { } ineligible)
            {
                Settle(index, ModelRouteOutcome.Ineligible, ineligible);
                _scope.FailedOver(ModelRouteOutcome.Ineligible);
                continue;
            }

            var admission = _health.Admit(candidate.HealthKey);
            if (admission.Refusal is { } refused)
            {
                var left = admission.Until - _time.GetUtcNow();
                Settle(index, refused, $"{Seconds(left)} s");
                _scope.FailedOver(refused);
                continue;
            }

            var attempt = new Attempt(index, admission.Test, CancellationTokenSource.CreateLinkedTokenSource(_cancellationToken));
            _all.Add(attempt);
            _running.Add(attempt);
            attempt.Task = RunAttemptAsync(attempt);
            return true;
        }

        return false;
    }

    /// <summary>
    /// One attempt: a model call of its own, under the route's activity. It never throws: whatever escapes the call's own
    /// handling (a scope that can't start, a bug) is the attempt's failure, with its model call completed, so the
    /// coordinator always settles it and releases its key's half-open test.
    /// </summary>
    private async Task<AttemptEnd> RunAttemptAsync(Attempt attempt)
    {
        try
        {
            return await AttemptAsync(attempt).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            try
            {
                attempt.Scope?.Failed(ex.GetType().Name, ex.Message);
                attempt.Scope?.Dispose();
            }
            catch (Exception)
            {
                // The model call's record is lost; the route's isn't.
            }

            return new AttemptEnd(AttemptKind.Failed, Error: ex, Fault: ModelFaults.Classify(ex));
        }
    }

    private async Task<AttemptEnd> AttemptAsync(Attempt attempt)
    {
        var candidate = _candidates[attempt.Index];
        if (_scope.Activity is { } route)
        {
            // Each attempt is a child of the route, whichever attempt was current when it started (a hedge's is).
            Activity.Current = route;
        }

        var options = _options?.Clone();
        if (candidate.Candidate.Model is { } model)
        {
            options ??= new ChatOptions();
            options.ModelId = model;
        }

        var scope = ModelCallScope.Start(
            _node,
            new ModelCallInfo(ModelOf(options?.ModelId, candidate), candidate.Provider)
            {
                MaxTokens = options?.MaxOutputTokens,
                Temperature = options?.Temperature,
                Agent = _agent,
                Route = _route.Name,
                Candidate = candidate.Candidate.Name,
            });
        attempt.Scope = scope;
        _callIds[attempt.Index] = scope.CallId;
        var token = attempt.Token;
        IAsyncEnumerator<ChatResponseUpdate>? stream = null;
        try
        {
            if (!_streaming)
            {
                var response = await candidate.Candidate.Client.GetResponseAsync(_messages, options, token).ConfigureAwait(false);
                scope.Succeeded(StrataChat.Usage(response.Usage), response.FinishReason?.Value, response.ModelId, response.ResponseId);
                scope.Dispose();
                return new AttemptEnd(AttemptKind.Answered, Response: response);
            }

            stream = candidate.Candidate.Client.GetStreamingResponseAsync(_messages, options, token).GetAsyncEnumerator(token);
            if (await stream.MoveNextAsync().ConfigureAwait(false))
            {
                scope.FirstChunk();
                return new AttemptEnd(AttemptKind.Started, First: stream.Current, Stream: stream, Scope: scope);
            }

            // A stream that ended without an update: an (empty) answer, judged like a whole one.
            await DisposeQuietlyAsync(stream).ConfigureAwait(false);
            var empty = Array.Empty<ChatResponseUpdate>().ToChatResponse();
            scope.Succeeded(ModelUsage.None, null);
            scope.Dispose();
            return new AttemptEnd(AttemptKind.Answered, Response: empty);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            await DisposeQuietlyAsync(stream).ConfigureAwait(false);
            scope.Cancelled();
            scope.Dispose();
            return new AttemptEnd(AttemptKind.Cancelled);
        }
        catch (Exception ex)
        {
            await DisposeQuietlyAsync(stream).ConfigureAwait(false);
            var fault = Classify(ex);
            scope.Failed(fault.ErrorType, ex.Message);
            scope.Dispose();
            return new AttemptEnd(AttemptKind.Failed, Error: ex, Fault: fault);
        }
    }

    /// <summary>What an attempt's end does to the call: <see langword="null"/> to go on, else the step that ends it.</summary>
    private async Task<RouteStep?> JudgeAsync(Attempt attempt, AttemptEnd end)
    {
        var candidate = _candidates[attempt.Index];
        switch (end.Kind)
        {
            case AttemptKind.Started:
                // The first stream to give an update wins, and the others are cancelled.
                _winner = attempt;
                attempt.Stream = end.Stream;
                attempt.Scope = end.Scope;
                _updates.Add(end.First!);
                await StopAsync(ModelRouteOutcome.LostRace).ConfigureAwait(false);
                return new RouteStep(RouteStepKind.Update, end.First);

            case AttemptKind.Cancelled:
                // Its token fired, but not the call's: nothing the route did, so it says nothing of the key.
                _health.Released(candidate.HealthKey, attempt.Test);
                Settle(attempt.Index, ModelRouteOutcome.Cancelled, null);
                return null;

            case AttemptKind.Failed when end.Fault!.IsTransient:
                _health.Failed(candidate.HealthKey, end.Fault.RetryAfter, attempt.Test);
                Settle(attempt.Index, ModelRouteOutcome.Failed, end.Fault.ErrorType);
                _scope.FailedOver(ModelRouteOutcome.Failed);
                return null;

            case AttemptKind.Failed:
                // The request is wrong, not the model: the route stops, and the fault is the call's as it is.
                _health.Released(candidate.HealthKey, attempt.Test);
                Settle(attempt.Index, ModelRouteOutcome.Failed, end.Fault.ErrorType);
                await StopAsync(ModelRouteOutcome.Cancelled).ConfigureAwait(false);
                return Finish(
                    new RouteStep(RouteStepKind.Failure, Error: end.Error, Fault: end.Fault), "fatal", null, null, end.Fault.ErrorType, end.Error!.Message);
        }

        var response = end.Response!;
        if (response.FinishReason == ChatFinishReason.ContentFilter)
        {
            _health.Released(candidate.HealthKey, attempt.Test);
            Settle(attempt.Index, ModelRouteOutcome.Refused, null);
            _scope.FailedOver(ModelRouteOutcome.Refused);
            return null;
        }

        string? rejected;
        try
        {
            rejected = _route.Options.Accept?.Invoke(response);
        }
        catch (Exception ex)
        {
            // The application's judge failed: a bug, not a model's answer to move on from.
            _health.Released(candidate.HealthKey, attempt.Test);
            Settle(attempt.Index, ModelRouteOutcome.Rejected, ex.Message);
            await StopAsync(ModelRouteOutcome.Cancelled).ConfigureAwait(false);
            return Finish(new RouteStep(RouteStepKind.Failure, Error: ex, Fault: Classify(ex)), "fatal", null, null, "accept", ex.Message);
        }

        if (rejected is not null)
        {
            _health.Released(candidate.HealthKey, attempt.Test);
            Settle(attempt.Index, ModelRouteOutcome.Rejected, rejected);
            _scope.FailedOver(ModelRouteOutcome.Rejected);
            return null;
        }

        _health.Succeeded(candidate.HealthKey, attempt.Test);
        Settle(attempt.Index, ModelRouteOutcome.Answered, null);
        await StopAsync(ModelRouteOutcome.LostRace).ConfigureAwait(false);
        return Finish(
            new RouteStep(RouteStepKind.Answer, Response: response), "answered", candidate.Candidate.Name, ModelOf(response, candidate), null, null);
    }

    /// <summary>Nothing left to ask: fatal when every candidate was ineligible, else transient with the shortest wait.</summary>
    private RouteStep Exhausted()
    {
        var parts = string.Join(", ", _candidates.Select((_, i) => Describe(i)));
        var attempts = Attempts();
        ModelRouteException failure;
        if (_outcomes.All(o => o == ModelRouteOutcome.Ineligible))
        {
            failure = new ModelRouteException(
                _route.Name,
                $"no candidate of route '{_route.Name}' can take this request: {parts}",
                isTransient: false,
                retryAfter: null,
                "route_ineligible",
                attempts);
            return Finish(new RouteStep(RouteStepKind.Failure, Error: failure, Fault: ModelFaults.Classify(failure)), "ineligible", null, null, failure.ErrorType, failure.Message);
        }

        failure = new ModelRouteException(
            _route.Name, $"no candidate of route '{_route.Name}' answered: {parts}", isTransient: true, ShortestWait(), "route_exhausted", attempts);
        return Finish(new RouteStep(RouteStepKind.Failure, Error: failure, Fault: ModelFaults.Classify(failure)), "exhausted", null, null, failure.ErrorType, failure.Message);
    }

    /// <summary>
    /// The shortest wait until some candidate that wasn't ineligible can be asked again; <see langword="null"/> when one
    /// can be now (the route could try it at once).
    /// </summary>
    private TimeSpan? ShortestWait()
    {
        TimeSpan? shortest = null;
        for (var i = 0; i < _candidates.Count; i++)
        {
            if (_outcomes[i] == ModelRouteOutcome.Ineligible)
            {
                continue;
            }

            var wait = _health.WaitFor(_candidates[i].HealthKey);
            if (wait <= TimeSpan.Zero)
            {
                return null;
            }

            shortest = shortest is { } s && s <= wait ? s : wait;
        }

        return shortest;
    }

    private RouteStep Committed(Attempt winner, string message, string errorType)
    {
        _health.Released(_candidates[winner.Index].HealthKey, winner.Test);
        var failure = new ModelRouteException(_route.Name, message, isTransient: true, retryAfter: null, errorType, Attempts());
        return Finish(new RouteStep(RouteStepKind.Failure, Error: failure, Fault: ModelFaults.Classify(failure)), "failed", null, null, errorType, message);
    }

    private async Task<RouteStep> CancelledAsync()
    {
        await StopAsync(ModelRouteOutcome.Cancelled).ConfigureAwait(false);
        return Finish(new RouteStep(RouteStepKind.Cancelled), "cancelled", null, null, "cancelled", "cancelled");
    }

    /// <summary>
    /// Cancels every attempt still running and records it as <paramref name="outcome"/>, releasing its key's half-open
    /// test; health is otherwise untouched.
    /// </summary>
    /// <remarks>
    /// The losers of a race (<see cref="ModelRouteOutcome.LostRace"/>) are not waited for: a client that ignores its
    /// token would hold the winner back. Each is reaped in the background (observed, its stream and token source
    /// disposed), and its model call records how it actually ended, which may be after <see cref="ModelRouted"/>. When
    /// the call itself stops (cancelled, a fatal fault), every attempt is waited for, so their model calls are completed
    /// before <see cref="ModelRouted"/> and inside the node.
    /// </remarks>
    private async Task StopAsync(ModelRouteOutcome outcome)
    {
        var running = _running.ToList();
        _running.Clear();
        foreach (var attempt in running)
        {
            await attempt.Cancellation.CancelAsync().ConfigureAwait(false);
        }

        foreach (var attempt in running)
        {
            if (outcome == ModelRouteOutcome.LostRace)
            {
                _health.Released(_candidates[attempt.Index].HealthKey, attempt.Test);
                Settle(attempt.Index, outcome, null);
                attempt.Reaped = true;
                _ = ReapAsync(attempt);
            }
            else
            {
                await AbandonAsync(attempt, await EndOf(attempt).ConfigureAwait(false), outcome).ConfigureAwait(false);
            }
        }
    }

    /// <summary>A loser left to end on its own: observed, its stream closed, its model call completed, its token source disposed.</summary>
    private static async Task ReapAsync(Attempt attempt)
    {
        try
        {
            var end = await EndOf(attempt).ConfigureAwait(false);
            if (end.Kind == AttemptKind.Started)
            {
                await DisposeQuietlyAsync(end.Stream).ConfigureAwait(false);
                end.Scope!.Cancelled();
                end.Scope.Dispose();
            }
        }
        catch (Exception)
        {
            // Nobody waits for a loser: what it does now changes nothing.
        }
        finally
        {
            attempt.Cancellation.Dispose();
        }
    }

    /// <summary>An attempt the call no longer wants: its stream closed, its call completed cancelled, its test released.</summary>
    private async Task AbandonAsync(Attempt attempt, AttemptEnd end, ModelRouteOutcome outcome)
    {
        if (end.Kind == AttemptKind.Started)
        {
            await DisposeQuietlyAsync(end.Stream).ConfigureAwait(false);
            end.Scope!.Cancelled();
            end.Scope.Dispose();
        }

        _health.Released(_candidates[attempt.Index].HealthKey, attempt.Test);
        Settle(attempt.Index, outcome, null);
    }

    /// <summary>Records the call's end, once: <see cref="ModelRouted"/>, the route's metrics and span, and <c>OnRouted</c>.</summary>
    private RouteStep Finish(RouteStep step, string outcome, string? answered, string? model, string? errorType, string? error)
    {
        _finished = true;
        var attempts = Attempts();
        var duration = _time.GetElapsedTime(_started);
        _scope.Completed(outcome, answered, model, attempts, _hedges, duration, errorType, error);
        try
        {
            _route.Options.OnRouted?.Invoke(new ModelRouteResult(_route.Name, answered, model, attempts, _hedges, duration));
        }
        catch (Exception)
        {
            // A ledger's failure is not the call's (design/46: it must not throw, and what it throws is ignored).
        }

        return step;
    }

    private void Settle(int index, ModelRouteOutcome outcome, string? reason)
    {
        _outcomes[index] = outcome;
        _reasons[index] = reason;
    }

    private List<ModelRouteAttempt> Attempts() =>
        [.. _candidates.Select((c, i) => new ModelRouteAttempt(c.Candidate.Name, _outcomes[i] ?? ModelRouteOutcome.NotTried, _reasons[i], _callIds[i]))];

    /// <summary>One candidate as the route's failure message names it (design/46, "The exact rules").</summary>
    private string Describe(int index)
    {
        var name = _candidates[index].Candidate.Name;
        var reason = _reasons[index];
        return _outcomes[index] switch
        {
            ModelRouteOutcome.Failed => $"{name} failed ({reason})",
            ModelRouteOutcome.Refused => $"{name} refused",
            ModelRouteOutcome.Rejected => $"{name}'s answer was rejected ({reason})",
            ModelRouteOutcome.Ineligible => $"{name} is ineligible ({reason})",
            ModelRouteOutcome.CoolingDown => $"{name} is cooling down for {reason}",
            ModelRouteOutcome.BreakerOpen => $"{name}'s breaker is open for {reason}",
            ModelRouteOutcome.Cancelled => $"{name} was cancelled",
            ModelRouteOutcome.LostRace => $"{name} lost the race",
            ModelRouteOutcome.Answered => $"{name} answered",
            _ => $"{name} was not tried",
        };
    }

    /// <summary>
    /// A candidate's fault: a <see cref="NodeException"/> as it says, anything else as <see cref="ModelFaults.Classify"/>
    /// reads it. A classifier that throws, or names no error type, is passed over for the default, so a faulty classifier
    /// can't leave an attempt unsettled.
    /// </summary>
    private ModelFault Classify(Exception exception)
    {
        if (exception is NodeException node)
        {
            var transient = node.Kind == NodeFailureKind.Transient;
            return new ModelFault(transient, node.RetryAfter, transient ? "transient" : "fatal");
        }

        try
        {
            var fault = ModelFaults.Classify(exception, _classifier);
            if (!string.IsNullOrWhiteSpace(fault.ErrorType))
            {
                return fault;
            }
        }
        catch (Exception)
        {
            // The application's classifier failed: the default reads the fault.
        }

        return ModelFaults.Classify(exception);
    }

    /// <summary>The model an attempt asks for, or <c>unknown</c>, as its model call names it.</summary>
    private static string ModelOf(string? requested, ModelRoute.Resolved candidate) =>
        !string.IsNullOrWhiteSpace(requested) ? requested
        : !string.IsNullOrWhiteSpace(candidate.DefaultModel) ? candidate.DefaultModel
        : "unknown";

    /// <summary>The model that answered: the response's, else the one asked for, else <c>unknown</c> (design/46).</summary>
    private string ModelOf(ChatResponse response, ModelRoute.Resolved candidate) =>
        !string.IsNullOrWhiteSpace(response.ModelId) ? response.ModelId : ModelOf(candidate.Candidate.Model ?? _options?.ModelId, candidate);

    /// <summary>An attempt's end; a task that faulted anyway (it shouldn't) is the attempt's failure.</summary>
    private static async Task<AttemptEnd> EndOf(Attempt attempt)
    {
        try
        {
            return await attempt.Task.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new AttemptEnd(AttemptKind.Failed, Error: ex, Fault: ModelFaults.Classify(ex));
        }
    }

    /// <summary>Seconds left, rounded up, as the messages give them.</summary>
    private static string Seconds(TimeSpan left) =>
        ((long)Math.Ceiling(Math.Max(0, left.TotalSeconds))).ToString(CultureInfo.InvariantCulture);

    private static async Task DisposeQuietlyAsync(IAsyncEnumerator<ChatResponseUpdate>? stream)
    {
        if (stream is null)
        {
            return;
        }

        try
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // A stream given up on: how it closes changes nothing.
        }
    }

    private enum AttemptKind
    {
        Answered,

        /// <summary>A stream gave its first update.</summary>
        Started,
        Failed,
        Cancelled,
    }

    private sealed record AttemptEnd(
        AttemptKind Kind,
        ChatResponse? Response = null,
        ChatResponseUpdate? First = null,
        Exception? Error = null,
        ModelFault? Fault = null,
        IAsyncEnumerator<ChatResponseUpdate>? Stream = null,
        ModelCallScope? Scope = null);

    /// <param name="test">Whether it is its health key's half-open test.</param>
    private sealed class Attempt(int index, bool test, CancellationTokenSource cancellation)
    {
        public int Index { get; } = index;

        public bool Test { get; } = test;

        /// <summary>Linked to the call's token, so cancelling the call cancels it, and the route can cancel it alone.</summary>
        public CancellationTokenSource Cancellation { get; } = cancellation;

        public CancellationToken Token { get; } = cancellation.Token;

        public Task<AttemptEnd> Task { get; set; } = null!;

        /// <summary>The winner's stream and model call, carried on after its first update.</summary>
        public IAsyncEnumerator<ChatResponseUpdate>? Stream { get; set; }

        public ModelCallScope? Scope { get; set; }

        /// <summary>A loser left to end in the background, which disposes its token source.</summary>
        public bool Reaped { get; set; }
    }
}
