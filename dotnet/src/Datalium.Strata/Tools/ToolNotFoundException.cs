namespace Datalium.Strata.Tools;

/// <summary>Thrown by generated <see cref="IStrataToolProvider.DispatchToolAsync(string, string)"/> implementations
/// and by <see cref="ToolRegistry"/> when asked to dispatch a tool name no <see cref="StrataToolAttribute"/>-annotated
/// method declared.</summary>
public class ToolNotFoundException : Exception
{
    public ToolNotFoundException(string toolName)
        : this(toolName, $"Tool '{toolName}' was not found.")
    {
    }

    protected ToolNotFoundException(string toolName, string message)
        : base(message)
    {
        ToolName = toolName;
    }

    public string ToolName { get; }
}

/// <summary>
/// Thrown by a <see cref="ToolRegistry"/> subset (<see cref="ToolRegistry.Subset(IEnumerable{string})"/>) asked
/// for a tool its parent has but the subset left out. Derives from <see cref="ToolNotFoundException"/>, so
/// code that tells a model "no such tool" handles it too; catch it first to tell the two apart.
/// </summary>
public sealed class ToolNotPermittedException(string toolName)
    : ToolNotFoundException(toolName, $"Tool '{toolName}' is not permitted here.");

/// <summary>
/// Thrown by a <see cref="IToolMiddleware"/> to refuse a call (<c>design/23-Tool-Middleware.md</c>): the
/// tool exists and the caller may use it, but a policy refused this call. Reported as a refused call
/// with the error type <c>denied</c>. Not a <see cref="ToolNotFoundException"/>, which tells a model the
/// tool doesn't exist.
/// </summary>
public sealed class ToolCallDeniedException(string toolName, string reason)
    : Exception($"Tool '{toolName}' was denied: {reason}")
{
    public string ToolName { get; } = toolName;

    /// <summary>Why the call was refused, as the middleware put it.</summary>
    public string Reason { get; } = reason;
}

/// <summary>Thrown by <see cref="ToolRegistry.Register(IStrataToolProvider)"/> for a tool name already registered.</summary>
public sealed class DuplicateToolException(string toolName)
    : InvalidOperationException($"Tool '{toolName}' is already registered.")
{
    public string ToolName { get; } = toolName;
}
