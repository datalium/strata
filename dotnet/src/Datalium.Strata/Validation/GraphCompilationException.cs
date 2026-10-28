namespace Datalium.Strata.Validation;

/// <summary>
/// Thrown by <see cref="Graph.StateGraph{TState}.Compile"/> when compile-time validation
/// (STRATA-SPEC-004) finds structural errors. Graphs with structural errors abort at startup,
/// not at runtime.
/// </summary>
public sealed class GraphCompilationException(ValidationResult result)
    : Exception(BuildMessage(result))
{
    public ValidationResult Result { get; } = result;

    private static string BuildMessage(ValidationResult result) =>
        "Graph compilation failed:\n" +
        string.Join('\n', result.Errors.Select(e => $"[{e.Code}] {e.Message}"));
}
