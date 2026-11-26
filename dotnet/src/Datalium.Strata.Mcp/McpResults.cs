using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Datalium.Strata.Mcp;

/// <summary>
/// An MCP result as the text a Strata tool returns (<c>design/50-MCP-Client.md</c>, "Results"), pinned by
/// <c>schema/fixtures/mcp/tool-results.json</c>.
/// </summary>
internal static class McpResults
{
    public const string ErrorWithoutMessage = "the tool reported an error without a message";

    /// <summary>The agent's own JSON for errors (<c>StrataAgent</c>): compact, with non-ASCII as it is.</summary>
    private static readonly JsonSerializerOptions ErrorJson = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private static readonly JsonWriterOptions CompactJson = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public static McpToolResult Read(JsonElement result)
    {
        var content = result.TryGetProperty("content", out var c) && c.ValueKind == JsonValueKind.Array
            ? c.EnumerateArray().Select(b => b.Clone()).ToArray()
            : [];
        JsonElement? structured = result.TryGetProperty("structuredContent", out var s) && s.ValueKind != JsonValueKind.Null
            ? s.Clone()
            : null;
        var isError = result.TryGetProperty("isError", out var e) && e.ValueKind == JsonValueKind.True;
        return new McpToolResult(content, structured, isError);
    }

    public static string ToText(McpToolResult result)
    {
        var text = result.StructuredContent is { } structured ? Compact(structured) : Blocks(result.Content);
        if (!result.IsError)
        {
            return text;
        }

        return JsonSerializer.Serialize(
            new Dictionary<string, string> { ["error"] = text.Length == 0 ? ErrorWithoutMessage : text }, ErrorJson);
    }

    private static string Blocks(IReadOnlyList<JsonElement> blocks) => string.Join("\n\n", blocks.Select(Block));

    private static string Block(JsonElement block)
    {
        var type = String(block, "type") ?? "unknown";
        switch (type)
        {
            case "text":
                return String(block, "text") ?? string.Empty;
            case "image" or "audio":
                return Placeholder(String(block, "mimeType"), String(block, "data"));
            case "resource_link":
                var uri = String(block, "uri") ?? string.Empty;
                return String(block, "name") is { } name ? $"[resource: {name} {uri}]" : $"[resource: {uri}]";
            case "resource" when block.TryGetProperty("resource", out var resource) && resource.ValueKind == JsonValueKind.Object:
                return String(resource, "text") ?? Placeholder(String(resource, "mimeType"), String(resource, "blob"));
            default:
                return $"[{type} content, not shown]";
        }
    }

    private static string Placeholder(string? mimeType, string? base64)
    {
        var characters = base64?.Count(ch => ch != '=' && !char.IsWhiteSpace(ch)) ?? 0;
        var bytes = (long)characters * 3 / 4;
        var kilobytes = (bytes + 1023) / 1024;
        return $"[{mimeType ?? "application/octet-stream"}, {kilobytes.ToString(CultureInfo.InvariantCulture)} KB, not shown]";
    }

    internal static string Compact(JsonElement element)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer, CompactJson))
        {
            element.WriteTo(writer);
        }

        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    private static string? String(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
}
