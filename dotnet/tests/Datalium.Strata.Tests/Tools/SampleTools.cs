using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests.Tools;

/// <summary>
/// Real <c>[StrataTool]</c> usage, compiled by the normal test-project build (not driven
/// manually through <c>CSharpGeneratorDriver</c>), so <see cref="StrataToolGeneratorTests"/>
/// exercises the actual generated <c>IStrataToolProvider</c> implementation end-to-end.
/// <c>CheckInventory</c> is the same conceptual tool as the Rust suite's
/// <c>check_inventory</c> (rust/strata-core/tests/tool_calling.rs), expressed with
/// idiomatic C# camelCase parameter naming rather than forcing byte-identical JSON keys
/// across languages. <c>GetUserBalance</c> reproduces STRATA-SPEC-003 section 3's own
/// example, including its optional `currency` parameter with a default value.
/// </summary>
public partial class SampleTools
{
    [StrataTool(Name = "check_inventory", Description = "Query warehouse item quantities.")]
    public static async Task<string> CheckInventoryAsync(string sku, int warehouseId)
    {
        return await Task.FromResult($"SKU {sku} count is 42 in Zone {warehouseId}");
    }

    [StrataTool(Name = "read_user_balance", Description = "Fetches the current ledger balance for a user ID.")]
    public static async Task<string> GetUserBalanceAsync(int userId, string currency = "USD")
    {
        return await Task.FromResult($"User {userId} balance in {currency}");
    }
}

/// <summary>
/// Optional nullable parameters (SG-33, <c>design/23-Tool-Middleware.md</c>): a <c>string?</c> with no
/// default, a <c>string?</c> with one, and an <c>int?</c>. Before SG-33 the first generated
/// <c>string app = null</c> (CS8625).
/// </summary>
public partial class OptionalTools
{
    [StrataTool(Name = "find_service", Description = "Finds a service, optionally filtered.")]
    public static string FindService(string? app, string? region = "eu", int? limit = null) =>
        $"app={app ?? "(none)"} region={region ?? "(none)"} limit={limit?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "(none)"}";
}
