using System.Text;
using System.Text.Json.Nodes;

namespace Datalium.Strata.DocsSnippets.InventoryMcp;

/// <summary>
/// The inventory MCP server of the samples for /guides/tools/mcp-servers/. It speaks the stateless 2026-07-28 revision
/// only, over stdio's framing (one JSON-RPC message per line) or one HTTP request at a time, and has three tools:
/// <c>lookup_stock</c> (text, or the tool's own error for an unknown SKU, or a JSON-RPC error without a SKU),
/// <c>list_warehouses</c> (structured content) and <c>reserve_stock</c>. The Rust twin answers the same.
/// </summary>
public sealed class InventoryServer(string warehouse)
{
    private static readonly Dictionary<string, Dictionary<string, int>> Stock = new()
    {
        ["east"] = new() { ["AB-12"] = 7, ["CD-34"] = 0 },
        ["west"] = new() { ["AB-12"] = 3 },
    };

    private const string Tools = """
        [
          {"name": "lookup_stock", "description": "Units in stock for a SKU.",
           "inputSchema": {"type": "object", "properties": {"sku": {"type": "string"}, "warehouse": {"type": "string"}}, "required": ["sku"]}},
          {"name": "list_warehouses", "description": "The warehouses this server knows.",
           "inputSchema": {"type": "object"}},
          {"name": "reserve_stock", "description": "Reserves units of a SKU for an order.",
           "inputSchema": {"type": "object", "properties": {"sku": {"type": "string"}, "units": {"type": "integer"}}, "required": ["sku", "units"]}}
        ]
        """;

    /// <summary>The warehouse a lookup uses when the call names none.</summary>
    public string Warehouse { get; } = warehouse;

    /// <summary>The warehouse <c>--warehouse &lt;name&gt;</c> names, else <c>east</c>.</summary>
    public static string WarehouseFrom(IReadOnlyList<string> args)
    {
        for (var i = 0; i < args.Count - 1; i++)
        {
            if (args[i] == "--warehouse")
            {
                return args[i + 1];
            }
        }

        return "east";
    }

    /// <summary>Serves one message per line until <paramref name="input"/> ends.</summary>
    public async Task ServeAsync(Stream input, Stream output)
    {
        using var reader = new StreamReader(input, new UTF8Encoding(false));
        await using var writer = new StreamWriter(output, new UTF8Encoding(false)) { AutoFlush = true, NewLine = "\n" };
        while (await reader.ReadLineAsync() is { } line)
        {
            if (line.Length > 0 && Answer(JsonNode.Parse(line)!.AsObject()) is { } answer)
            {
                await writer.WriteLineAsync(answer.ToJsonString());
            }
        }
    }

    /// <summary>The answer to one JSON-RPC message, or null for a notification, which gets none.</summary>
    public JsonObject? Answer(JsonObject message)
    {
        if (message["id"] is not { } id)
        {
            return null;
        }

        var answer = new JsonObject { ["jsonrpc"] = "2.0", ["id"] = id.DeepClone() };
        var method = message["method"]?.GetValue<string>();
        var parameters = message["params"] as JsonObject;
        try
        {
            answer["result"] = method switch
            {
                "server/discover" => new JsonObject
                {
                    ["resultType"] = "complete",
                    ["supportedVersions"] = new JsonArray("2026-07-28"),
                    ["capabilities"] = new JsonObject { ["tools"] = new JsonObject() },
                    ["_meta"] = new JsonObject
                    {
                        ["io.modelcontextprotocol/serverInfo"] = new JsonObject { ["name"] = "inventory-mcp", ["version"] = "1.0.0" },
                    },
                },
                "tools/list" => new JsonObject { ["tools"] = JsonNode.Parse(Tools) },
                "tools/call" => Call(parameters?["name"]?.GetValue<string>(), parameters?["arguments"] as JsonObject ?? []),
                _ => throw new Fault(-32601, $"Method not found: {method}"),
            };
        }
        catch (Fault fault)
        {
            answer["error"] = new JsonObject { ["code"] = fault.Code, ["message"] = fault.Message };
        }

        return answer;
    }

    private JsonObject Call(string? tool, JsonObject arguments)
    {
        switch (tool)
        {
            case "lookup_stock":
                var sku = Sku(arguments);
                var warehouse = arguments["warehouse"]?.GetValue<string>() ?? Warehouse;
                return Stock.GetValueOrDefault(warehouse)?.TryGetValue(sku, out var units) == true
                    ? Text($"{sku}: {units} units in {warehouse}")
                    : Text($"no SKU '{sku}' in {warehouse}", isError: true);
            case "list_warehouses":
                var names = new JsonArray([.. Stock.Keys.Select(k => (JsonNode)JsonValue.Create(k))]);
                var listed = new JsonObject { ["warehouses"] = names };
                var result = Text(listed.ToJsonString());
                result["structuredContent"] = listed.DeepClone();
                return result;
            case "reserve_stock":
                return Text($"reserved {arguments["units"]?.GetValue<int>() ?? 0} units of {Sku(arguments)} in {Warehouse}");
            default:
                throw new Fault(-32602, $"Unknown tool: {tool}");
        }
    }

    private static string Sku(JsonObject arguments) =>
        arguments["sku"]?.GetValue<string>() ?? throw new Fault(-32602, "Invalid params: sku is required");

    private static JsonObject Text(string text, bool isError = false)
    {
        var result = new JsonObject
        {
            ["content"] = new JsonArray(new JsonObject { ["type"] = "text", ["text"] = text }),
        };
        if (isError)
        {
            result["isError"] = true;
        }

        return result;
    }

    private sealed class Fault(int code, string message) : Exception(message)
    {
        public int Code { get; } = code;
    }
}
