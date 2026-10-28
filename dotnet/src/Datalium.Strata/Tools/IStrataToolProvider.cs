namespace Datalium.Strata.Tools;

/// <summary>
/// Implemented by the generated partial-class output of
/// <c>Datalium.Strata.SourceGenerators</c> for any <c>partial class</c>
/// declaring one or more <see cref="StrataToolAttribute"/>-annotated methods,
/// and by <see cref="ToolRegistry"/>.
/// </summary>
public interface IStrataToolProvider
{
    /// <summary>The JSON-Schema manifest array for every tool this provider declares.</summary>
    string GetToolsManifest();

    /// <summary>
    /// Deserializes <paramref name="jsonArguments"/> and invokes the tool named
    /// <paramref name="toolName"/> directly — a static dispatch, not a reflective lookup.
    /// </summary>
    Task<string> DispatchToolAsync(string toolName, string jsonArguments);

    /// <summary>
    /// <see cref="DispatchToolAsync(string, string)"/> with the call's context and token
    /// (<c>design/20-Tool-Dispatch-And-Registry.md</c>), which a <c>[StrataTool]</c> method receives by
    /// declaring a <see cref="ToolCallContext"/> and/or <see cref="CancellationToken"/> parameter.
    /// </summary>
    /// <remarks>
    /// Generated providers implement it. The default ignores the context and the token, for providers
    /// written by hand before it existed.
    /// </remarks>
    Task<string> DispatchToolAsync(
        string toolName, string jsonArguments, ToolCallContext context, CancellationToken cancellationToken = default)
        => DispatchToolAsync(toolName, jsonArguments);
}
