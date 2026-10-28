using System.Diagnostics;
using Datalium.Strata.Graph;

namespace Datalium.Strata.Observability;

/// <summary>
/// What a model call asks for, as its <see cref="ModelCallScope"/> records it (<c>design/44-LLM-Integration.md</c>).
/// The messages are not part of it: Strata never records prompts or answers.
/// </summary>
/// <param name="Model">The model asked for; <c>unknown</c> when the caller doesn't know.</param>
/// <param name="Provider">The provider (<c>openai</c>, <c>anthropic</c>, …), when known: <c>gen_ai.provider.name</c>.</param>
public sealed record ModelCallInfo(string Model, string? Provider = null)
{
    /// <summary>
    /// The GenAI operation: <c>chat</c>, or <c>embeddings</c> for an embedding call (<c>design/45-Long-Term-Memory.md</c>).
    /// It names the span (<c>&lt;operation&gt; &lt;model&gt;</c>), tags <c>gen_ai.operation.name</c>, and is the events' <c>Operation</c>.
    /// </summary>
    public string Operation { get; init; } = "chat";

    /// <summary>The request's output-token limit, when it set one.</summary>
    public int? MaxTokens { get; init; }

    /// <summary>The request's temperature, when it set one.</summary>
    public double? Temperature { get; init; }

    /// <summary>The agent making the call, when an agent does (<c>strata.agent</c>).</summary>
    public string? Agent { get; init; }

    /// <summary>
    /// The model route the call is an attempt of (<c>design/46-Model-Routes.md</c>), <see langword="null"/> outside a
    /// route: <c>strata.route</c> on the span, and <c>Route</c> on the call's events.
    /// </summary>
    public string? Route { get; init; }

    /// <summary>The route's candidate making the call, when <see cref="Route"/> is set: <c>strata.route.candidate</c>.</summary>
    public string? Candidate { get; init; }
}

/// <summary>
/// One model call, from <see cref="ModelCallStarted"/> to <see cref="ModelCallCompleted"/>
/// (<c>design/44-LLM-Integration.md</c>): the way a package that calls models (<c>Datalium.Strata.AI</c>, or an
/// application's own client) puts the call in its run's events and the engine's telemetry, as a
/// <c>ToolRegistry</c> does for a tool call. It opens a <c>strata.model</c> activity named <c>&lt;operation&gt; &lt;model&gt;</c>
/// with the OpenTelemetry GenAI attributes, and records <c>gen_ai.client.token.usage</c> and
/// <c>gen_ai.client.operation.duration</c>.
/// </summary>
/// <remarks>
/// The outcome is recorded once, by whichever of <see cref="Succeeded"/>, <see cref="Failed"/> or
/// <see cref="Cancelled"/> comes first; disposing a scope that has none records <see cref="Cancelled"/>, then
/// stops the activity. Started without a node (<see langword="null"/>), it records no run event.
/// </remarks>
public sealed class ModelCallScope : IDisposable
{
    private readonly ModelCallInfo _info;
    private readonly NodeContext? _node;
    private readonly long _started = Stopwatch.GetTimestamp();
    private int _firstChunk;
    private int _completed;

    private ModelCallScope(NodeContext? node, ModelCallInfo info)
    {
        _node = node;
        _info = info;
        CallId = Guid.CreateVersion7().ToString();
        // A child of whatever is current (inside a node, its attempt's activity), and of the node's activity
        // when nothing is (a flow that didn't carry Activity.Current).
        Activity = System.Diagnostics.Activity.Current is null && node?.Activity is { } parent
            ? StrataTelemetry.Source.StartActivity("strata.model", ActivityKind.Client, parent.Context)
            : StrataTelemetry.Source.StartActivity("strata.model", ActivityKind.Client);
        if (Activity is not null)
        {
            Activity.DisplayName = $"{info.Operation} {info.Model}";
            Activity.SetTag("gen_ai.operation.name", info.Operation);
            Activity.SetTag("gen_ai.provider.name", info.Provider);
            Activity.SetTag("gen_ai.request.model", info.Model);
            Activity.SetTag("gen_ai.request.max_tokens", info.MaxTokens);
            Activity.SetTag("gen_ai.request.temperature", info.Temperature);
            Activity.SetTag("strata.model.call_id", CallId);
            Activity.SetTag("strata.agent", info.Agent);
            if (info.Route is not null)
            {
                Activity.SetTag("strata.route", info.Route);
                Activity.SetTag("strata.route.candidate", info.Candidate);
            }

            if (node is not null)
            {
                Activity.SetTag("strata.thread_id", node.ThreadId);
                Activity.SetTag("strata.run_id", string.IsNullOrEmpty(node.RunId) ? null : node.RunId);
                Activity.SetTag("strata.node.path", node.NodePath);
                Activity.SetTag("strata.attempt", node.Attempt);
            }
        }

        if (node?.Telemetry is { Observed: true } run)
        {
            run.Record(
                new ModelCallStarted(info.Model, CallId, info.Provider, info.Route, info.Operation),
                node.NodePath,
                node.Attempt);
        }
    }

    /// <summary>The call's activity; <see langword="null"/> when nothing listens to <c>Datalium.Strata</c>.</summary>
    public Activity? Activity { get; }

    /// <summary>The call's id (a UUID v7), on both its events and its activity (<c>strata.model.call_id</c>).</summary>
    public string CallId { get; }

    /// <summary>
    /// Starts recording a model call. <paramref name="node"/> is the calling node's context, which puts the call in
    /// that run's events; <see langword="null"/> outside a run.
    /// </summary>
    public static ModelCallScope Start(NodeContext? node, ModelCallInfo info)
    {
        ArgumentNullException.ThrowIfNull(info);
        ArgumentException.ThrowIfNullOrWhiteSpace(info.Model);
        return new ModelCallScope(node, info);
    }

    /// <summary>A streamed call's first update arrived: records <c>strata.model.time_to_first_chunk</c> once.</summary>
    public void FirstChunk()
    {
        if (Interlocked.Exchange(ref _firstChunk, 1) == 1)
        {
            return;
        }

        StrataTelemetry.ModelTimeToFirstChunk.Record(Stopwatch.GetElapsedTime(_started).TotalSeconds, Tags(null, null));
    }

    /// <summary>The call answered.</summary>
    /// <param name="usage">The tokens the provider reported.</param>
    /// <param name="finishReason">Why the model stopped, as the provider said (<c>stop</c>, <c>length</c>, …).</param>
    /// <param name="responseModel">The model that answered, when the provider said.</param>
    /// <param name="responseId">The provider's id for the answer.</param>
    public void Succeeded(ModelUsage usage, string? finishReason, string? responseModel = null, string? responseId = null)
    {
        ArgumentNullException.ThrowIfNull(usage);
        if (!Begin())
        {
            return;
        }

        if (Activity is not null)
        {
            Activity.SetTag("gen_ai.response.model", responseModel);
            Activity.SetTag("gen_ai.response.id", responseId);
            if (finishReason is not null)
            {
                Activity.SetTag("gen_ai.response.finish_reasons", new[] { finishReason });
            }

            Activity.SetTag("gen_ai.usage.input_tokens", usage.InputTokens);
            Activity.SetTag("gen_ai.usage.output_tokens", usage.OutputTokens);
        }

        if (usage.InputTokens is { } input)
        {
            var tags = Tags(responseModel, null);
            tags.Add("gen_ai.token.type", "input");
            StrataTelemetry.ModelTokenUsage.Record(input, tags);
        }

        if (usage.OutputTokens is { } output)
        {
            var tags = Tags(responseModel, null);
            tags.Add("gen_ai.token.type", "output");
            StrataTelemetry.ModelTokenUsage.Record(output, tags);
        }

        End(ModelCallResult.Succeeded, usage, finishReason, responseModel, null, null);
    }

    /// <summary>The call failed.</summary>
    /// <param name="errorType">A low-cardinality class: the HTTP status (<c>429</c>), <c>timeout</c>,
    /// <c>connection</c>, <c>stream_broken</c>, or an exception's type name. <c>error.type</c>.</param>
    public void Failed(string errorType, string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(errorType);
        ArgumentNullException.ThrowIfNull(message);
        if (!Begin())
        {
            return;
        }

        StrataTelemetry.Fail(Activity, message, errorType);
        End(ModelCallResult.Failed, ModelUsage.None, null, null, message, errorType);
    }

    /// <summary>The caller's token stopped the call.</summary>
    public void Cancelled()
    {
        if (!Begin())
        {
            return;
        }

        StrataTelemetry.Fail(Activity, "cancelled", "cancelled");
        End(ModelCallResult.Cancelled, ModelUsage.None, null, null, null, "cancelled");
    }

    /// <summary>Records <see cref="Cancelled"/> if no outcome was recorded, then stops the activity.</summary>
    public void Dispose()
    {
        Cancelled();
        Activity?.Dispose();
    }

    private bool Begin() => Interlocked.Exchange(ref _completed, 1) == 0;

    private void End(
        ModelCallResult result, ModelUsage usage, string? finishReason, string? responseModel, string? error, string? errorType)
    {
        var elapsed = Stopwatch.GetElapsedTime(_started);
        StrataTelemetry.ModelDuration.Record(elapsed.TotalSeconds, Tags(responseModel, errorType));
        if (_node?.Telemetry is { Observed: true } run)
        {
            run.Record(
                new ModelCallCompleted(
                    _info.Model, CallId, result, usage, finishReason, elapsed, error, _info.Route, _info.Operation),
                _node.NodePath,
                _node.Attempt);
        }
    }

    // Bounded attributes only: no thread, run, node path or call id on metrics (design/18).
    private TagList Tags(string? responseModel, string? errorType)
    {
        var tags = new TagList
        {
            { "gen_ai.operation.name", _info.Operation },
            { "gen_ai.request.model", _info.Model },
        };
        if (_info.Provider is not null)
        {
            tags.Add("gen_ai.provider.name", _info.Provider);
        }

        if (responseModel is not null)
        {
            tags.Add("gen_ai.response.model", responseModel);
        }

        if (errorType is not null)
        {
            tags.Add("error.type", errorType);
        }

        return tags;
    }
}
