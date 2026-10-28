using System.Text.Json.Nodes;

namespace Datalium.Strata.Interruption;

/// <summary>
/// Which graph wrote a checkpoint level (<c>design/31-Graph-Identity.md</c>): the id and optional version a
/// graph was compiled with (<c>CompilationOptions.GraphId</c> / <c>GraphVersion</c>). Both are free text,
/// compared exactly: versions have no order. The checkpoint's <c>graph</c> object, and a definition's
/// <c>identity</c>. Mirrors the Rust engine's <c>GraphIdentity</c>.
/// </summary>
public sealed record GraphIdentity(string Id, string? Version)
{
    /// <summary>This identity as its JSON object: both keys always written.</summary>
    public JsonObject ToWireJson() => new()
    {
        ["id"] = Id,
        ["version"] = Version,
    };

    /// <summary>The inverse of <see cref="ToWireJson"/>.</summary>
    public static GraphIdentity FromWireJson(JsonNode node)
    {
        var json = node.AsObject();
        return new GraphIdentity(json["id"]!.GetValue<string>(), json["version"]?.GetValue<string>());
    }

    /// <summary>For messages: <c>'incident' version 2</c>, or <c>'incident' unversioned</c>.</summary>
    public override string ToString() => $"'{Id}' {VersionText(Version)}";

    /// <summary>For messages: <c>version 2</c>, or <c>unversioned</c>.</summary>
    internal static string VersionText(string? version) => version is null ? "unversioned" : $"version {version}";
}
