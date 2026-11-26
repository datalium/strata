using System.Globalization;
using System.Reflection;
using System.Text.Json;

namespace Datalium.Strata.Mcp;

/// <summary>The protocol's names and numbers, and the era rule (<c>design/50-MCP-Client.md</c>, "Opening a connection").</summary>
internal static class McpProtocol
{
    public const string Modern = "2026-07-28";

    /// <summary>The legacy versions Strata speaks on stdio, newest first; HTTP leaves out the last.</summary>
    public static readonly string[] LegacyStdio = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

    public static readonly string[] LegacyHttp = ["2025-11-25", "2025-06-18", "2025-03-26"];

    public const string MetaProtocolVersion = "io.modelcontextprotocol/protocolVersion";
    public const string MetaClientCapabilities = "io.modelcontextprotocol/clientCapabilities";
    public const string MetaClientInfo = "io.modelcontextprotocol/clientInfo";
    public const string MetaServerInfo = "io.modelcontextprotocol/serverInfo";

    public const int HeaderMismatch = -32020;
    public const int MissingRequiredClientCapability = -32021;
    public const int UnsupportedProtocolVersion = -32022;
    public const int MethodNotFound = -32601;

    public static readonly string StrataVersion =
        typeof(McpProtocol).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0]
        ?? typeof(McpProtocol).Assembly.GetName().Version?.ToString(3)
        ?? "0.0.0";

    public static bool IsModernError(int code) => code is HeaderMismatch or MissingRequiredClientCapability or UnsupportedProtocolVersion;

    public static string[] Legacy(bool http) => http ? LegacyHttp : LegacyStdio;

    public static bool KnowsLegacy(string version, bool http) => Array.IndexOf(Legacy(http), version) >= 0;

    /// <summary>
    /// The era of a server from its answer to <c>server/discover</c>: a response (<see langword="null"/> for silence,
    /// stdio only). Replayed against <c>schema/fixtures/mcp/era-detection.json</c>.
    /// </summary>
    public static EraOutcome Classify(RpcResponse? answer, bool http)
    {
        if (answer is null)
        {
            return EraOutcome.Legacy(Legacy(http)[0]);
        }

        if (answer.Error is { } error)
        {
            switch (error.Code)
            {
                case UnsupportedProtocolVersion:
                    return Choose(SupportedOf(error.Data), http);
                case HeaderMismatch or MissingRequiredClientCapability:
                    return EraOutcome.Fail($"mcp error {error.Code.ToString(CultureInfo.InvariantCulture)}: {error.Message}");
                default:
                    return EraOutcome.Legacy(Legacy(http)[0]);
            }
        }

        var result = answer.Result;
        var versions = result.ValueKind == JsonValueKind.Object && result.TryGetProperty("supportedVersions", out var listed)
            ? Strings(listed)
            : null;
        return versions is not null && versions.Contains(Modern, StringComparer.Ordinal)
            ? EraOutcome.ModernEra
            : Choose(versions, http);
    }

    /// <summary>The era from an HTTP answer to <c>server/discover</c>: its status, and the JSON-RPC message its body
    /// held, if any.</summary>
    public static EraOutcome ClassifyHttp(int status, RpcResponse? message)
    {
        if (status is >= 200 and <= 299)
        {
            return message is null
                ? EraOutcome.Fail($"the MCP server answered server/discover with HTTP {status} and no JSON-RPC response")
                : Classify(message, http: true);
        }

        if (status is 401 or 403)
        {
            return EraOutcome.Fail($"unauthorized ({status})");
        }

        if (status >= 500 || status < 400)
        {
            return EraOutcome.Fail($"the MCP server answered server/discover with HTTP {status}");
        }

        return message?.Error is { } error && IsModernError(error.Code)
            ? Classify(message, http: true)
            : EraOutcome.Legacy(LegacyHttp[0]);
    }

    private static EraOutcome Choose(IReadOnlyList<string>? supported, bool http)
    {
        if (supported is not null)
        {
            foreach (var version in Legacy(http))
            {
                if (supported.Contains(version, StringComparer.Ordinal))
                {
                    return EraOutcome.Legacy(version);
                }
            }
        }

        var theirs = supported is { Count: > 0 } ? string.Join(", ", supported) : "none it named";
        var ours = string.Join(", ", [Modern, .. Legacy(http)]);
        return EraOutcome.Fail($"the MCP server supports no protocol version Strata speaks: it supports {theirs}, and Strata {ours}");
    }

    private static IReadOnlyList<string>? SupportedOf(JsonElement? data) =>
        data is { ValueKind: JsonValueKind.Object } d && d.TryGetProperty("supported", out var supported) ? Strings(supported) : null;

    private static List<string>? Strings(JsonElement element) =>
        element.ValueKind == JsonValueKind.Array
            ? element.EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!).ToList()
            : null;
}

/// <summary>What the probe decided: <see cref="Era"/> is <c>modern</c>, <c>legacy</c> (with the version to
/// <c>initialize</c> with) or <c>fail</c> (with why).</summary>
internal sealed record EraOutcome(string Era, string? Version, string? Error)
{
    public static readonly EraOutcome ModernEra = new("modern", McpProtocol.Modern, null);

    public static EraOutcome Legacy(string version) => new("legacy", version, null);

    public static EraOutcome Fail(string error) => new("fail", null, error);
}

/// <summary>A JSON-RPC error object.</summary>
internal sealed record RpcError(int Code, string Message, JsonElement? Data);

/// <summary>A JSON-RPC response: a result, or an error.</summary>
internal sealed record RpcResponse(long? Id, JsonElement Result, RpcError? Error)
{
    /// <summary>
    /// <paramref name="message"/> as a response, or <see langword="null"/> when it is a request or a notification.
    /// An error whose shape is wrong is still an error, with code 0.
    /// </summary>
    public static RpcResponse? From(JsonElement message)
    {
        if (message.ValueKind != JsonValueKind.Object || message.TryGetProperty("method", out _))
        {
            return null;
        }

        long? id = message.TryGetProperty("id", out var i) && i.ValueKind == JsonValueKind.Number && i.TryGetInt64(out var n) ? n : null;
        if (message.TryGetProperty("error", out var e) && e.ValueKind == JsonValueKind.Object)
        {
            var code = e.TryGetProperty("code", out var c) && c.ValueKind == JsonValueKind.Number && c.TryGetInt32(out var k) ? k : 0;
            var text = e.TryGetProperty("message", out var m) && m.ValueKind == JsonValueKind.String ? m.GetString()! : string.Empty;
            JsonElement? data = e.TryGetProperty("data", out var d) ? d.Clone() : null;
            return new RpcResponse(id, default, new RpcError(code, text, data));
        }

        if (message.TryGetProperty("result", out var result))
        {
            return new RpcResponse(id, result.Clone(), null);
        }

        return null;
    }

    /// <summary>The first JSON-RPC response in <paramref name="text"/> (one JSON object), or <see langword="null"/>.</summary>
    public static RpcResponse? Parse(string text)
    {
        try
        {
            using var document = JsonDocument.Parse(text);
            return From(document.RootElement);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
