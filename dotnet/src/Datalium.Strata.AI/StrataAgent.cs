using System.Text.Json;
using Datalium.Strata.Graph;
using Datalium.Strata.Observability;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>What a tool failure that isn't a refusal does (<c>design/44-LLM-Integration.md</c>, "A refused or failed tool call").</summary>
public enum ToolFailures
{
    /// <summary>
    /// The node fails: a <see cref="NodeException"/> the tool threw as it is, anything else transient or fatal as
    /// <see cref="ModelFaults.Classify"/> reads it (a timeout or an HTTP fault is transient).
    /// </summary>
    FailNode,

    /// <summary>The model is told, as <c>{"error": "…"}</c>, and the loop goes on.</summary>
    ReturnToModel,
}

/// <summary>An agent's options (<see cref="StrataAgent{TOutput}"/>). Mirrors the Rust <c>AgentOptions</c>.</summary>
public sealed record StrataAgentOptions<TOutput>
{
    /// <summary>The agent's id: the tool calls' <see cref="ToolCallContext.CallerId"/>, the turns' <c>strata.agent</c>, and the name its failures give.</summary>
    public required string Id { get; init; }

    /// <summary>Every turn's request options, copied (model, temperature, …); their tools and response format are the agent's.</summary>
    public ChatOptions? Chat { get; init; }

    /// <summary>The most turns with tool calls; one more fails the node transiently. Repair turns don't count. 8.</summary>
    public int MaxTurns { get; init; } = 8;

    /// <summary>How many times an answer that doesn't parse or validate is sent back to be repaired, over the whole run; then the node fails transiently. 1.</summary>
    public int MaxRepairs { get; init; } = 1;

    /// <summary>How many tool calls of one turn run at once: 1 (the default) runs them one after another, in the model's order.</summary>
    public int ToolConcurrency { get; init; } = 1;

    /// <summary>What a tool failure that isn't a refusal does. <see cref="AI.ToolFailures.FailNode"/> by default.</summary>
    public ToolFailures ToolFailures { get; init; } = ToolFailures.FailNode;

    /// <summary>Checks every parsed answer: the problems with it, none when it is valid. Problems are sent back to be repaired.</summary>
    public Func<TOutput, IReadOnlyList<string>>? Validate { get; init; }

    /// <summary>
    /// How answers are read and the schema is made. <see cref="AIJsonUtilities.DefaultOptions"/> with
    /// <c>RespectNullableAnnotations</c> and <c>RespectRequiredConstructorParameters</c> when not given, so an answer
    /// missing a field it can't do without is sent back to be repaired.
    /// </summary>
    public JsonSerializerOptions? JsonOptions { get; init; }

    /// <summary>Reads a provider's or a tool's own exceptions before the default does.</summary>
    public IModelFaultClassifier? FaultClassifier { get; init; }

    /// <summary>
    /// Changes one turn's options before its model call: the turn's own copy of <see cref="Chat"/>, with the agent's tools
    /// and response format already set. For example, require a tool call on the first turn only, which
    /// <see cref="Chat"/> can't do, since it applies to every turn and a turn that must call a tool never answers:
    /// <c>ConfigureTurn = (turn, chat) =&gt; { if (turn.Number == 1) chat.ToolMode = ChatToolMode.RequireAny; }</c>.
    /// Replacing the tools or the response format is the caller's to get right: the agent still maps the model's tool
    /// names through <see cref="StrataAgent{TOutput}.Declarations"/> and reads the answer as <typeparamref name="TOutput"/>.
    /// </summary>
    public Action<AgentTurn, ChatOptions>? ConfigureTurn { get; init; }
}

/// <summary>A turn of an agent's run, as <see cref="StrataAgentOptions{TOutput}.ConfigureTurn"/> sees it before the model call.</summary>
/// <param name="Number">The turn's number, from 1; repair turns count.</param>
/// <param name="ToolTurns">How many turns before this one answered with tool calls.</param>
/// <param name="Repairs">How many repair requests have been sent, this turn's included.</param>
/// <param name="IsRepair">Whether this turn answers a repair request.</param>
/// <param name="Conversation">The messages this turn sends.</param>
public sealed record AgentTurn(int Number, int ToolTurns, int Repairs, bool IsRepair, IReadOnlyList<ChatMessage> Conversation);

/// <summary>One tool call an agent made, as the model saw it answered.</summary>
/// <param name="Name">The Strata name (the model's name when no declaration had it).</param>
/// <param name="CallId">The model's id for the call.</param>
/// <param name="Result">
/// <see cref="ToolCallResult.Succeeded"/>; <see cref="ToolCallResult.Refused"/> (not found, not permitted, denied,
/// arguments that don't parse); <see cref="ToolCallResult.Failed"/> (a failure sent back to the model under
/// <see cref="ToolFailures.ReturnToModel"/>).
/// </param>
public sealed record AgentToolCall(string Name, string CallId, ToolCallResult Result);

/// <summary>What an agent's run produced. Mirrors the Rust <c>AgentResult</c>.</summary>
/// <param name="Output">The parsed, validated answer (a text agent's: the answer's text).</param>
/// <param name="Messages">The whole conversation: the messages given, then every turn's answer, tool results and repair request.</param>
/// <param name="Usage">Summed over the turns; a count no turn reported stays <see langword="null"/>.</param>
/// <param name="Turns">Model calls made, repair turns included.</param>
/// <param name="ToolCalls">Every tool call, in the order the model asked for them.</param>
public sealed record AgentResult<TOutput>(
    TOutput Output, IReadOnlyList<ChatMessage> Messages, ModelUsage Usage, int Turns, IReadOnlyList<AgentToolCall> ToolCalls);

/// <summary>
/// An agent (<c>design/44-LLM-Integration.md</c>): a value a node runs, not a node kind. Mirrors the Rust <c>Agent</c>.
/// </summary>
/// <remarks>
/// <para>
/// Each turn is one model call through <see cref="StrataChat"/> (so it streams when the run does, is observed, and fails
/// as any call fails) with the registry's tools under provider-safe names (<see cref="ToolDeclarations"/>). The tool
/// calls in an answer are mapped back to their Strata names and dispatched through the registry, its middleware
/// included, with <c>ToolCallContext.From(context) with { CallerId = Id, CallId = the model's id }</c>; their results
/// go back in one tool message, in the model's order. An answer without tool calls is the answer: parsed into
/// <typeparamref name="TOutput"/> (a surrounding Markdown fence removed), validated, and sent back to be repaired when
/// it is bad.
/// </para>
/// <para>
/// <typeparamref name="TOutput"/>'s JSON schema (<see cref="AIJsonUtilities.CreateJsonSchema"/>, every property required
/// and no others allowed) is the response format; <c>StrataAgent&lt;string&gt;</c> is a text agent, whose answer is its
/// text. The conversation lives for one run of the agent (one attempt of its node): a retry starts over.
/// </para>
/// </remarks>
public sealed class StrataAgent<TOutput>
{
    private static readonly bool IsText = typeof(TOutput) == typeof(string);

    private static readonly JsonSerializerOptions DefaultJson = new(AIJsonUtilities.DefaultOptions)
    {
        RespectNullableAnnotations = true,
        RespectRequiredConstructorParameters = true,
    };

    private readonly IChatClient _client;
    private readonly ToolRegistry _tools;
    private readonly StrataAgentOptions<TOutput> _options;
    private readonly JsonSerializerOptions _json;
    private readonly StrataChatOptions _strata;

    public StrataAgent(IChatClient client, ToolRegistry tools, StrataAgentOptions<TOutput> options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(tools);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Id, nameof(options));
        ArgumentOutOfRangeException.ThrowIfNegative(options.MaxTurns, nameof(options));
        ArgumentOutOfRangeException.ThrowIfNegative(options.MaxRepairs, nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.ToolConcurrency, 1, nameof(options));
        _client = client;
        _tools = tools;
        _options = options;
        _json = options.JsonOptions ?? DefaultJson;
        _strata = new StrataChatOptions { FaultClassifier = options.FaultClassifier, Agent = options.Id };
        Declarations = ToolDeclarations.For(tools);
        if (!IsText)
        {
            var schema = AIJsonUtilities.CreateJsonSchema(
                typeof(TOutput),
                serializerOptions: _json,
                inferenceOptions: new AIJsonSchemaCreateOptions
                {
                    TransformOptions = new AIJsonSchemaTransformOptions
                    {
                        DisallowAdditionalProperties = true,
                        RequireAllProperties = true,
                        MoveDefaultKeywordToDescription = true,
                    },
                });
            ResponseFormat = ChatResponseFormat.ForJsonSchema(schema, ToolDeclarations.ProviderName(typeof(TOutput).Name));
        }
    }

    /// <summary>The agent's id.</summary>
    public string Id => _options.Id;

    /// <summary>The agent's options.</summary>
    public StrataAgentOptions<TOutput> Options => _options;

    /// <summary>The tools as the model sees them.</summary>
    public ToolDeclarations Declarations { get; }

    /// <summary>The response format every turn asks for: <see langword="null"/> for a text agent.</summary>
    public ChatResponseFormat? ResponseFormat { get; }

    /// <summary>Runs the agent on <paramref name="messages"/> from inside the node <paramref name="context"/> is.</summary>
    /// <exception cref="NodeException">
    /// A model call's fault, as <see cref="StrataChat"/> throws it; transient after more than <c>MaxTurns</c> turns with
    /// tool calls (<c>agent 'x' made 8 tool turns without answering</c>) or an answer still bad after <c>MaxRepairs</c>
    /// repairs (<c>agent 'x' gave no valid answer: …</c>); a tool's failure under <see cref="ToolFailures.FailNode"/>.
    /// The node's own cancellation is rethrown as it is.
    /// </exception>
    public async Task<AgentResult<TOutput>> RunAsync(
        IEnumerable<ChatMessage> messages, NodeContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(context);
        var conversation = new List<ChatMessage>(messages);
        var usage = ModelUsage.None;
        var (turns, toolTurns, repairs) = (0, 0, 0);
        var repairing = false;
        var toolCalls = new List<AgentToolCall>();
        while (true)
        {
            var chat = _options.Chat?.Clone() ?? new ChatOptions();
            chat.Tools = Declarations.Tools.Count == 0 ? null : [.. Declarations.Tools];
            chat.ResponseFormat = ResponseFormat;
            _options.ConfigureTurn?.Invoke(new AgentTurn(turns + 1, toolTurns, repairs, repairing, conversation.AsReadOnly()), chat);
            var response = await StrataChat.GetResponseAsync(_client, conversation, chat, context, _strata, cancellationToken)
                .ConfigureAwait(false);
            turns++;
            usage = Add(usage, StrataChat.Usage(response.Usage));
            conversation.AddRange(response.Messages);
            var calls = response.Messages.SelectMany(m => m.Contents).OfType<FunctionCallContent>().ToList();

            if (calls.Count > 0)
            {
                if (++toolTurns > _options.MaxTurns)
                {
                    throw NodeException.Transient($"agent '{Id}' made {_options.MaxTurns} tool turns without answering");
                }

                var answered = await CallToolsAsync(calls, context, cancellationToken).ConfigureAwait(false);
                conversation.Add(new ChatMessage(
                    ChatRole.Tool, [.. answered.Select(a => (AIContent)new FunctionResultContent(a.Record.CallId, a.Content))]));
                toolCalls.AddRange(answered.Select(a => a.Record));
                repairing = false;
                continue;
            }

            var problems = Read(response.Text, out var output);
            if (problems.Count == 0)
            {
                return new AgentResult<TOutput>(output!, conversation, usage, turns, toolCalls);
            }

            var listed = string.Join("; ", problems);
            if (repairs >= _options.MaxRepairs)
            {
                throw NodeException.Transient($"agent '{Id}' gave no valid answer: {listed}");
            }

            repairs++;
            repairing = true;
            conversation.Add(new ChatMessage(
                ChatRole.User,
                IsText
                    ? $"Your answer was not valid: {listed}. Answer again."
                    : $"Your answer was not valid: {listed}. Answer again with one JSON object that matches the schema."));
        }
    }

    /// <summary>The answer's text as <typeparamref name="TOutput"/>, and what is wrong with it.</summary>
    private IReadOnlyList<string> Read(string text, out TOutput? output)
    {
        if (IsText)
        {
            output = (TOutput)(object)text;
        }
        else
        {
            output = default;
            var body = Unfenced(text);
            if (body.Length == 0)
            {
                return ["it is empty"];
            }

            try
            {
                output = JsonSerializer.Deserialize<TOutput>(body, _json);
            }
            catch (JsonException ex)
            {
                return [$"it is not JSON that matches the schema ({ex.Message})"];
            }

            if (output is null)
            {
                return ["it is not JSON that matches the schema (it is null)"];
            }
        }

        return _options.Validate?.Invoke(output) ?? [];
    }

    /// <summary>
    /// A turn's calls in the model's order, up to <c>ToolConcurrency</c> at once; the first failure in that order ends
    /// the turn, cancelling the rest.
    /// </summary>
    private async Task<IReadOnlyList<Answered>> CallToolsAsync(
        List<FunctionCallContent> calls, NodeContext context, CancellationToken cancellationToken)
    {
        if (_options.ToolConcurrency == 1 || calls.Count == 1)
        {
            var answered = new List<Answered>(calls.Count);
            foreach (var call in calls)
            {
                answered.Add(await CallToolAsync(call, context, cancellationToken).ConfigureAwait(false));
            }

            return answered;
        }

        using var gate = new SemaphoreSlim(_options.ToolConcurrency);
        using var siblings = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var running = calls.Select(async call =>
        {
            await gate.WaitAsync(siblings.Token).ConfigureAwait(false);
            try
            {
                return await CallToolAsync(call, context, siblings.Token).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        }).ToList();
        var results = new List<Answered>(calls.Count);
        try
        {
            foreach (var call in running)
            {
                results.Add(await call.ConfigureAwait(false));
            }
        }
        catch
        {
            await siblings.CancelAsync().ConfigureAwait(false);
            try
            {
                await Task.WhenAll(running).ConfigureAwait(false);
            }
            catch
            {
                // The failure that ended the turn is the one that counts.
            }

            throw;
        }

        return results;
    }

    /// <summary>One tool call: what goes back to the model and the record of it, or the node's failure.</summary>
    private async Task<Answered> CallToolAsync(FunctionCallContent call, NodeContext context, CancellationToken cancellationToken)
    {
        var strata = Declarations.StrataName(call.Name);
        Answered Answer(ToolCallResult result, string content) => new(new AgentToolCall(strata, call.CallId, result), content);
        Answered Refused(string message) => Answer(ToolCallResult.Refused, ErrorJson(message));

        if (call.Exception is not null)
        {
            return Refused($"the arguments for tool '{call.Name}' aren't a JSON object");
        }

        var arguments = JsonSerializer.Serialize(
            call.Arguments ?? new Dictionary<string, object?>(), AIJsonUtilities.DefaultOptions.GetTypeInfo(typeof(IDictionary<string, object?>)));
        var toolContext = ToolCallContext.From(context) with
        {
            CallerId = Id,
            CallId = string.IsNullOrEmpty(call.CallId) ? null : call.CallId,
        };
        try
        {
            var result = await _tools.DispatchToolAsync(strata, arguments, toolContext, cancellationToken).ConfigureAwait(false);
            return Answer(ToolCallResult.Succeeded, result);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (ToolNotPermittedException)
        {
            return Refused($"tool '{call.Name}' is not permitted here");
        }
        catch (ToolNotFoundException)
        {
            return Refused($"tool '{call.Name}' was not found");
        }
        catch (ToolCallDeniedException denied)
        {
            return Refused($"denied: {denied.Reason}");
        }
        catch (ToolArgumentsException invalid)
        {
            // Arguments the tool can't take, refused before it ran. A tool's own exception, an ArgumentException
            // included, is the tool's failure.
            return Refused($"invalid arguments for tool '{call.Name}': {invalid.Problems}");
        }
        catch (Exception ex)
        {
            if (_options.ToolFailures == ToolFailures.ReturnToModel)
            {
                return Answer(ToolCallResult.Failed, ErrorJson($"tool '{call.Name}' failed: {ex.Message}"));
            }

            if (ex is NodeException)
            {
                throw;
            }

            var fault = ModelFaults.Classify(ex, _options.FaultClassifier);
            var message = $"tool '{strata}' failed: {ex.Message}";
            throw fault.IsTransient ? NodeException.Transient(message, fault.RetryAfter, ex) : NodeException.Fatal(message, ex);
        }
    }

    private static string ErrorJson(string message) =>
        JsonSerializer.Serialize(new Dictionary<string, string> { ["error"] = message }, ErrorJsonOptions);

    private static readonly JsonSerializerOptions ErrorJsonOptions = new()
    {
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static ModelUsage Add(ModelUsage total, ModelUsage turn) =>
        new(Sum(total.InputTokens, turn.InputTokens), Sum(total.OutputTokens, turn.OutputTokens));

    private static long? Sum(long? a, long? b) => a is null && b is null ? null : (a ?? 0) + (b ?? 0);

    /// <summary>The text inside a Markdown code fence, if the model wrapped its answer in one; the text trimmed otherwise.</summary>
    internal static string Unfenced(string text)
    {
        var trimmed = text.Trim();
        if (!trimmed.StartsWith("```", StringComparison.Ordinal))
        {
            return trimmed;
        }

        var start = trimmed.IndexOf('\n', StringComparison.Ordinal);
        var end = trimmed.LastIndexOf("```", StringComparison.Ordinal);
        return start >= 0 && end > start ? trimmed[(start + 1)..end].Trim() : trimmed;
    }

    private sealed record Answered(AgentToolCall Record, string Content);
}
