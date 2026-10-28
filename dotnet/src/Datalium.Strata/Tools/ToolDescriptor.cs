using System.Text.Json;

namespace Datalium.Strata.Tools;

/// <summary>
/// One entry of a manifest, parsed: the shared <c>name</c> / <c>description</c> / <c>parameters</c>
/// shape. <see cref="ToolRegistry.Tools"/> exposes these so a renderer for a model provider's tool
/// format is a few lines of mapping (<c>design/20-Tool-Dispatch-And-Registry.md</c>, "Manifest renderers").
/// </summary>
/// <param name="Parameters">The JSON-Schema object of the tool's parameters.</param>
public sealed record ToolDescriptor(string Name, string Description, JsonElement Parameters);
