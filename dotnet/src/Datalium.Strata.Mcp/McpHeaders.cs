using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Datalium.Strata.Mcp;

/// <summary>
/// The modern Streamable HTTP headers taken from a request's body (<c>design/50-MCP-Client.md</c>, "Streamable HTTP"):
/// the value encoding (<c>schema/fixtures/mcp/header-values.json</c>) and the <c>x-mcp-header</c> rules
/// (<c>schema/fixtures/mcp/x-mcp-header.json</c>).
/// </summary>
internal static class McpHeaders
{
    public const string Annotation = "x-mcp-header";

    private const string SentinelStart = "=?base64?";
    private const string SentinelEnd = "?=";
    private const long MaxSafeInteger = 9007199254740991;

    private const string Unreachable = "an x-mcp-header isn't reachable from the schema root through properties only";

    /// <summary>Keywords whose value maps names to schemas.</summary>
    private static readonly HashSet<string> SchemaMaps = new(StringComparer.Ordinal)
    {
        "$defs", "definitions", "patternProperties", "dependentSchemas",
    };

    /// <summary>Keywords whose value is an instance, not a schema.</summary>
    private static readonly HashSet<string> Values = new(StringComparer.Ordinal) { "const", "enum", "default", "examples" };

    /// <summary><paramref name="text"/> as a header value: as it is when that is safe, else in the Base64 sentinel form.</summary>
    public static string Encode(string text)
    {
        var plain = text.Length == 0 || (text[0] != ' ' && text[^1] != ' ' && text.All(ch => ch is >= ' ' and <= '~'));
        if (plain && !(text.StartsWith(SentinelStart, StringComparison.Ordinal) && text.EndsWith(SentinelEnd, StringComparison.Ordinal)))
        {
            return text;
        }

        return SentinelStart + Convert.ToBase64String(Encoding.UTF8.GetBytes(text)) + SentinelEnd;
    }

    /// <summary>A parameter value as a header value, or <see langword="null"/> when it gives no header.</summary>
    public static string? Value(JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.String:
                return Encode(value.GetString()!);
            case JsonValueKind.True:
                return "true";
            case JsonValueKind.False:
                return "false";
            case JsonValueKind.Number:
                var raw = value.GetRawText();
                if (raw.AsSpan().IndexOfAny(".eE") >= 0 || !value.TryGetInt64(out var integer) || integer is > MaxSafeInteger or < -MaxSafeInteger)
                {
                    return null;
                }

                return integer.ToString(CultureInfo.InvariantCulture);
            default:
                return null;
        }
    }

    /// <summary>
    /// The <c>x-mcp-header</c> annotations of a tool's <paramref name="inputSchema"/>, or why the tool must be left
    /// out.
    /// </summary>
    public static (IReadOnlyList<HeaderParameter> Parameters, string? Rejected) Read(JsonElement inputSchema)
    {
        var found = new List<HeaderParameter>();
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        string? rejected = null;

        void Walk(JsonElement schema, bool reachable, List<string> path)
        {
            if (rejected is not null || schema.ValueKind != JsonValueKind.Object)
            {
                return;
            }

            foreach (var keyword in schema.EnumerateObject())
            {
                if (rejected is not null)
                {
                    return;
                }

                var value = keyword.Value;
                if (keyword.Name == Annotation)
                {
                    rejected = reachable && path.Count > 0 ? Check(schema, value, path, names, found) : Unreachable;
                }
                else if (keyword.Name == "properties")
                {
                    if (value.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var property in value.EnumerateObject())
                        {
                            Walk(property.Value, reachable, [.. path, property.Name]);
                        }
                    }
                }
                else if (SchemaMaps.Contains(keyword.Name))
                {
                    if (value.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var entry in value.EnumerateObject())
                        {
                            Walk(entry.Value, false, path);
                        }
                    }
                }
                else if (Values.Contains(keyword.Name))
                {
                    continue;
                }
                else if (value.ValueKind == JsonValueKind.Object)
                {
                    Walk(value, false, path);
                }
                else if (value.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in value.EnumerateArray())
                    {
                        Walk(item, false, path);
                    }
                }
            }
        }

        Walk(inputSchema, true, []);
        return (rejected is null ? found : [], rejected);
    }

    /// <summary>The <c>Mcp-Param-*</c> headers for a call's <paramref name="arguments"/>.</summary>
    public static IEnumerable<KeyValuePair<string, string>> For(IReadOnlyList<HeaderParameter> parameters, JsonElement arguments)
    {
        foreach (var parameter in parameters)
        {
            var value = arguments;
            var present = true;
            foreach (var name in parameter.Path)
            {
                if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(name, out value))
                {
                    present = false;
                    break;
                }
            }

            if (present && Value(value) is { } header)
            {
                yield return new("Mcp-Param-" + parameter.Name, header);
            }
        }
    }

    private static string? Check(JsonElement schema, JsonElement annotation, List<string> path, HashSet<string> names, List<HeaderParameter> found)
    {
        var where = string.Join('.', path);
        if (annotation.ValueKind != JsonValueKind.String || annotation.GetString() is not { Length: > 0 } name || !name.All(IsTokenChar))
        {
            return $"x-mcp-header on '{where}' isn't a valid header name";
        }

        var type = schema.TryGetProperty("type", out var t) ? t : default;
        if (type.ValueKind != JsonValueKind.String || type.GetString() is not ("string" or "integer" or "boolean"))
        {
            var shown = type.ValueKind == JsonValueKind.Undefined ? "untyped" : McpResults.Compact(type);
            return $"x-mcp-header on '{where}' is on a {shown} property: only string, integer and boolean can be headers";
        }

        if (!names.Add(name))
        {
            return $"x-mcp-header '{name}' is used twice";
        }

        found.Add(new HeaderParameter(name, [.. path]));
        return null;
    }

    /// <summary>RFC 9110 <c>tchar</c>.</summary>
    private static bool IsTokenChar(char ch) => char.IsAsciiLetterOrDigit(ch) || "!#$%&'*+-.^_`|~".Contains(ch);
}

/// <summary>A parameter mirrored into <c>Mcp-Param-{Name}</c>: the header's name, and the property path to its value.</summary>
internal sealed record HeaderParameter(string Name, IReadOnlyList<string> Path);
