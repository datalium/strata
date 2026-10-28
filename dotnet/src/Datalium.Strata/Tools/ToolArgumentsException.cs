using System.Text.Json;

namespace Datalium.Strata.Tools;

/// <summary>
/// Thrown by a generated <see cref="IStrataToolProvider"/> when a call's arguments don't fit the tool: they aren't a
/// JSON object, a required argument is missing, or one has the wrong type (<c>design/20-Tool-Dispatch-And-Registry.md</c>,
/// "Arguments a tool can't read"). Thrown before the tool runs, so it is never the tool's own failure: an agent refuses
/// the call back to the model with <see cref="Problems"/>, and a <see cref="ToolRegistry"/> records it with the error
/// type <c>invalid_arguments</c>. A hand-written provider throws it for the same reasons. It is an
/// <see cref="ArgumentException"/>, so code that caught that before still does.
/// </summary>
public sealed class ToolArgumentsException : ArgumentException
{
    public ToolArgumentsException(string toolName, string problems, Exception? innerException = null)
        : base($"Invalid arguments for tool '{toolName}': {problems}", innerException)
    {
        ToolName = toolName;
        Problems = problems;
    }

    public string ToolName { get; }

    /// <summary>What is wrong, in words a model can act on: <c>argument 'orderId' is missing</c>.</summary>
    public string Problems { get; }

    /// <summary>
    /// The exception for <paramref name="arguments"/> that failed to deserialize with <paramref name="error"/>:
    /// <see cref="ToolArguments.Problems"/> against <paramref name="parametersSchema"/> (the tool's manifest
    /// <c>parameters</c>), or, when they explain nothing, the property the parser stopped at. Used by the generated
    /// dispatch.
    /// </summary>
    public static ToolArgumentsException From(string toolName, string arguments, string parametersSchema, JsonException error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new(toolName, ToolArguments.Problems(arguments, parametersSchema) ?? Unexplained(arguments, error), error);
    }

    // The parser's own message names the generated arguments record and a position in text the caller never wrote,
    // so only the property it stopped at is kept: `$.count` → `argument 'count' isn't valid`, or `… can't be null`
    // for null given to an optional parameter that isn't nullable (`string currency = "USD"`).
    private static string Unexplained(string arguments, JsonException error)
    {
        var path = error.Path;
        if (path is null || !path.StartsWith("$.", StringComparison.Ordinal) || path.Length == 2)
        {
            return "they don't match the tool's parameters";
        }

        var end = path.IndexOfAny(['.', '['], 2);
        var name = end < 0 ? path[2..] : path[2..end];
        using var document = JsonDocument.Parse(arguments);
        return document.RootElement.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Null
            ? $"argument '{name}' can't be null"
            : $"argument '{name}' isn't valid";
    }
}

/// <summary>
/// What a tool call's arguments got wrong against its parameters' JSON schema, in the words both engines use
/// (<c>schema/fixtures/tools/argument-problems.json</c>). Mirrors Rust's <c>tools::argument_problems</c>.
/// </summary>
public static class ToolArguments
{
    /// <summary>
    /// The problems with <paramref name="arguments"/> (raw JSON text) against <paramref name="parametersSchema"/> (a JSON
    /// object schema with <c>properties</c> and <c>required</c>), joined with <c>; </c>, or <see langword="null"/> when
    /// they match it. Text that isn't JSON: <c>the arguments aren't valid JSON</c>; JSON that isn't an object:
    /// <c>the arguments aren't a JSON object</c>. Otherwise each property of the schema, in byte order of its name: a
    /// required one absent is <c>argument 'x' is missing</c>, null is fine unless it is required, and a value not of the
    /// property's <c>type</c> is <c>argument 'x' must be a string</c> (an integer, a number, true or false, an array, an
    /// object). Properties the schema doesn't name, and a property without a known type, aren't checked.
    /// </summary>
    public static string? Problems(string arguments, string parametersSchema)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        ArgumentNullException.ThrowIfNull(parametersSchema);
        using var schema = JsonDocument.Parse(parametersSchema);
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(arguments);
        }
        catch (JsonException)
        {
            return "the arguments aren't valid JSON";
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return "the arguments aren't a JSON object";
            }

            if (!schema.RootElement.TryGetProperty("properties", out var properties)
                || properties.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            var required = new HashSet<string>(StringComparer.Ordinal);
            if (schema.RootElement.TryGetProperty("required", out var names) && names.ValueKind == JsonValueKind.Array)
            {
                foreach (var name in names.EnumerateArray())
                {
                    if (name.GetString() is { } value)
                    {
                        required.Add(value);
                    }
                }
            }

            var problems = new List<string>();
            foreach (var property in properties.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal))
            {
                var name = property.Name;
                var type = property.Value.ValueKind == JsonValueKind.Object
                    && property.Value.TryGetProperty("type", out var t) && t.ValueKind == JsonValueKind.String
                        ? t.GetString()
                        : null;
                if (!root.TryGetProperty(name, out var value))
                {
                    if (required.Contains(name))
                    {
                        problems.Add($"argument '{name}' is missing");
                    }

                    continue;
                }

                if (Expected(type) is not { } expected
                    || (value.ValueKind == JsonValueKind.Null && !required.Contains(name))
                    || Fits(value, type!))
                {
                    continue;
                }

                problems.Add($"argument '{name}' must be {expected}");
            }

            return problems.Count == 0 ? null : string.Join("; ", problems);
        }
    }

    private static string? Expected(string? type) => type switch
    {
        "string" => "a string",
        "integer" => "an integer",
        "number" => "a number",
        "boolean" => "true or false",
        "array" => "an array",
        "object" => "an object",
        _ => null,
    };

    private static bool Fits(JsonElement value, string type) => type switch
    {
        "string" => value.ValueKind == JsonValueKind.String,
        "integer" => value.ValueKind == JsonValueKind.Number && (value.TryGetInt64(out _) || value.TryGetUInt64(out _)),
        "number" => value.ValueKind == JsonValueKind.Number,
        "boolean" => value.ValueKind is JsonValueKind.True or JsonValueKind.False,
        "array" => value.ValueKind == JsonValueKind.Array,
        "object" => value.ValueKind == JsonValueKind.Object,
        _ => true,
    };
}
