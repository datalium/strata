using System.Text.Json;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests.Tools;

/// <summary>
/// Mirrors <c>rust/strata-core/tests/tool_registry.rs</c> (<c>design/20-Tool-Dispatch-And-Registry.md</c>,
/// SG-21): duplicate names refused at registration, one merged manifest in registration order, dispatch
/// by name, and curated subsets that refuse what they left out as "not permitted". Keep the two in step.
/// </summary>
public class ToolRegistryTests
{
    private static ToolRegistry Abc() => new ToolRegistry().Register(new AbcTools());

    private static List<string> ManifestNames(IStrataToolProvider tools) =>
        [.. JsonDocument.Parse(tools.GetToolsManifest()).RootElement.EnumerateArray()
            .Select(e => e.GetProperty("name").GetString()!)];

    private static string Result(string json) => JsonDocument.Parse(json).RootElement.GetProperty("result").GetString()!;

    [Fact]
    public void The_Manifest_Merges_Every_Tool_In_Registration_Order()
    {
        var tools = new ToolRegistry()
            .Register(new ContextTools(new ContextTools.Probe()))
            .Register(new AbcTools());

        Assert.Equal(["probe", "wait", "mixed", "plain", "foreign_cancel", "alpha", "beta", "gamma"], ManifestNames(tools));
        Assert.Equal(ManifestNames(tools), tools.Tools.Select(t => t.Name));
        var alpha = tools.Tools.Single(t => t.Name == "alpha");
        Assert.Equal("First.", alpha.Description);
        Assert.Equal("integer", alpha.Parameters.GetProperty("properties").GetProperty("x").GetProperty("type").GetString());
    }

    [Fact]
    public async Task A_Duplicate_Name_Is_Refused_At_Registration_And_Registers_Nothing()
    {
        var tools = Abc();

        var refused = Assert.Throws<DuplicateToolException>(() => tools.Register(new OtherAlphaTools()));

        Assert.Equal("alpha", refused.ToolName);
        Assert.Equal(["alpha", "beta", "gamma"], ManifestNames(tools));
        Assert.Equal("alpha 1", Result(await tools.DispatchToolAsync("alpha", """{"x":1}""")));
    }

    [Fact]
    public async Task Dispatch_Routes_By_Name_And_Refuses_Unknown_Names()
    {
        var tools = Abc();

        Assert.Equal("beta 2", Result(await tools.DispatchToolAsync("beta", """{"x":2}""")));
        Assert.Equal("gamma 3", Result(await tools.DispatchToolAsync("gamma", """{"x":3}""")));
        var refused = await Assert.ThrowsAsync<ToolNotFoundException>(() => tools.DispatchToolAsync("delta", "{}"));
        Assert.Equal("delta", refused.ToolName);
    }

    [Fact]
    public async Task A_Subset_Holds_Only_The_Named_Tools_In_The_Parents_Order()
    {
        var subset = Abc().Subset("gamma", "alpha");

        Assert.Equal(["alpha", "gamma"], ManifestNames(subset));
        Assert.Equal("gamma 4", Result(await subset.DispatchToolAsync("gamma", """{"x":4}""")));
    }

    [Fact]
    public void A_Subset_Refuses_Unknown_Names_When_It_Is_Made()
    {
        var refused = Assert.Throws<ToolNotFoundException>(() => Abc().Subset("alpha", "delta"));

        Assert.Equal("delta", refused.ToolName);
    }

    [Fact]
    public async Task A_Subset_Refuses_A_Tool_It_Left_Out_As_Not_Permitted_And_An_Unknown_One_As_Not_Found()
    {
        var subset = Abc().Subset("alpha");

        var leftOut = await Assert.ThrowsAsync<ToolNotPermittedException>(() => subset.DispatchToolAsync("beta", """{"x":1}"""));
        var unknown = await Assert.ThrowsAsync<ToolNotFoundException>(() => subset.DispatchToolAsync("delta", """{"x":1}"""));

        Assert.Equal("beta", leftOut.ToolName);
        Assert.IsType<ToolNotFoundException>(unknown, exactMatch: true);
    }

    [Fact]
    public async Task A_Subset_Of_A_Subset_Can_Only_Narrow()
    {
        var first = Abc().Subset("alpha", "beta");
        var second = first.Subset("beta");

        Assert.Equal(["beta"], ManifestNames(second));
        foreach (var name in new[] { "alpha", "gamma" })
        {
            var refused = await Assert.ThrowsAsync<ToolNotPermittedException>(() => second.DispatchToolAsync(name, """{"x":1}"""));
            Assert.Equal(name, refused.ToolName);
        }

        // Widening back to a tool the first subset left out is refused the same way.
        Assert.Equal("gamma", Assert.Throws<ToolNotPermittedException>(() => first.Subset("gamma")).ToolName);
    }

    [Fact]
    public void A_Subset_Is_A_Snapshot_Of_Its_Parent()
    {
        var parent = new ToolRegistry().Register(new OtherAlphaTools());
        var subset = parent.Subset("alpha");

        parent.Register(new ContextTools(new ContextTools.Probe()));

        Assert.Equal(["alpha"], ManifestNames(subset));
    }

    [Fact]
    public async Task A_Repeated_Name_Counts_Once_And_An_Empty_Subset_Is_Allowed()
    {
        var repeated = Abc().Subset("beta", "beta");
        var empty = Abc().Subset();

        Assert.Equal(["beta"], ManifestNames(repeated));
        Assert.Equal("[]", empty.GetToolsManifest());
        await Assert.ThrowsAsync<ToolNotPermittedException>(() => empty.DispatchToolAsync("alpha", """{"x":1}"""));
    }

    [Fact]
    public async Task Registering_A_Registry_Adds_Its_Tools_Not_Itself()
    {
        var inner = Abc().Subset("alpha");
        var outer = new ToolRegistry().Register(inner).Register(new LegacyProvider());

        Assert.Equal(["alpha", "legacy"], ManifestNames(outer));
        Assert.Equal("alpha 5", Result(await outer.DispatchToolAsync("alpha", """{"x":5}""")));
        Assert.Throws<DuplicateToolException>(() => outer.Register(new AbcTools()));
    }

    [Fact]
    public async Task A_Hand_Written_Provider_Registers_And_Dispatches_Through_The_Default_Overload()
    {
        var tools = new ToolRegistry().Register(new LegacyProvider());

        Assert.Equal("legacy", await tools.DispatchToolAsync("legacy", "{}", new ToolCallContext { CallerId = "a" }));
        Assert.Equal("Old.", Assert.Single(tools.Tools).Description);
    }

    [Fact]
    public async Task Registering_While_Calls_Are_In_Flight_Is_Safe()
    {
        var tools = Abc();
        var registering = Task.Run(() =>
        {
            for (var i = 0; i < 200; i++)
            {
                tools.Register(new NumberedProvider(i));
            }
        });
        var calling = Task.Run(async () =>
        {
            for (var i = 0; i < 200; i++)
            {
                Assert.Equal($"alpha {i}", Result(await tools.DispatchToolAsync("alpha", $$"""{"x":{{i}}}""")));
            }
        });

        await Task.WhenAll(registering, calling);

        Assert.Equal(203, tools.Tools.Count);
    }

    /// <summary>A hand-written provider with one tool named <c>n&lt;number&gt;</c>.</summary>
    private sealed class NumberedProvider(int number) : IStrataToolProvider
    {
        public string GetToolsManifest() => $$$"""[{"name":"n{{{number}}}","description":"","parameters":{}}]""";

        public Task<string> DispatchToolAsync(string toolName, string jsonArguments) => Task.FromResult(toolName);
    }
}
