namespace Datalium.Strata.Tools;

/// <summary>
/// Marks a method as an LLM-callable tool. <c>Datalium.Strata.SourceGenerators</c>
/// discovers methods carrying this attribute (by its full metadata name, without
/// referencing this assembly) inside a <c>partial class</c> and emits a matching
/// JSON-Schema manifest entry plus a static dispatch case for it — see
/// STRATA-SPEC-003.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class StrataToolAttribute : Attribute
{
    public required string Name { get; init; }

    public required string Description { get; init; }
}
