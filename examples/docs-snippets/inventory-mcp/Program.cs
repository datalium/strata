using Datalium.Strata.DocsSnippets.InventoryMcp;

// inventory-mcp [--warehouse <name>]: the inventory MCP server over stdio. It writes one line to stderr first, which a
// client's log shows, saying whether INVENTORY_API_KEY reached it.
var server = new InventoryServer(InventoryServer.WarehouseFrom(args));
var key = Environment.GetEnvironmentVariable("INVENTORY_API_KEY") is { Length: > 0 } ? "an API key" : "no API key";
await Console.Error.WriteLineAsync($"inventory-mcp: serving {server.Warehouse}, with {key}");
await Console.Error.FlushAsync();
await server.ServeAsync(Console.OpenStandardInput(), Console.OpenStandardOutput());
