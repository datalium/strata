namespace Datalium.Strata.Tools;

/// <summary>
/// One tool call as a <see cref="IToolMiddleware"/> sees it, after the registry resolved the name
/// (<c>design/23-Tool-Middleware.md</c>). Derive a changed call with <c>with</c>:
/// <c>call with { Context = call.Context with { CallerId = "…" } }</c>. Mirrors the Rust engine's
/// <c>ToolCall</c>.
/// </summary>
/// <remarks>
/// <see cref="Name"/> and <see cref="Descriptor"/> have no <c>init</c> accessor, so <c>with</c> can't
/// change them: the end of the chain always runs the tool the registry resolved.
/// </remarks>
public sealed record ToolCall
{
    private readonly ToolCallContext _context;

    public ToolCall(string name, string arguments, ToolCallContext context, ToolDescriptor? descriptor = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(context);
        Name = name;
        Arguments = arguments;
        _context = context;
        Descriptor = descriptor;
    }

    /// <summary>The tool's name.</summary>
    public string Name { get; }

    /// <summary>The call's JSON arguments, which a middleware may replace (redaction, normalization).</summary>
    public string Arguments { get; init; }

    /// <summary>The call's context, carrying the <c>strata.tool</c> span when nothing else replaced it.</summary>
    public ToolCallContext Context
    {
        get => _context;
        init => _context = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>The tool's manifest entry; <see langword="null"/> for a call the registry refused.</summary>
    public ToolDescriptor? Descriptor { get; }
}

/// <summary>The rest of a tool call's middleware chain, ending at the tool (<c>design/23-Tool-Middleware.md</c>).</summary>
public delegate Task<string> ToolCallDelegate(ToolCall call, CancellationToken cancellationToken);

/// <summary>
/// Runs around every tool call a <see cref="ToolRegistry"/> dispatches (<c>design/23-Tool-Middleware.md</c>),
/// registered with <see cref="ToolRegistry.Use(IToolMiddleware)"/>: outermost first, inside the call's
/// <c>strata.tool</c> span. Mirrors the Rust engine's <c>ToolMiddleware</c>.
/// </summary>
/// <remarks>
/// A middleware may call <c>next</c> (once, more than once, or with a changed call or token), return a
/// result without calling it (a short-circuit, reported succeeded), throw
/// <see cref="ToolCallDeniedException"/> to refuse the call (reported refused, <c>denied</c>), or throw
/// anything else to fail it.
/// </remarks>
public interface IToolMiddleware
{
    /// <summary>Handles <paramref name="call"/>, continuing the chain with <paramref name="next"/> or not.</summary>
    Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken);

    /// <summary>
    /// Told of a call the registry refused before the chain ran: a name it doesn't have
    /// (<see cref="ToolNotFoundException"/>) or that its subset left out (<see cref="ToolNotPermittedException"/>).
    /// Runs in registration order, even when the token has fired, and can't turn the refusal into a
    /// result. An exception thrown here ends the call in place of the refusal, and the hooks after it
    /// don't run. The default does nothing.
    /// </summary>
    Task OnRefusedAsync(ToolCall call, ToolNotFoundException refusal, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
