using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Storage.Schema;

namespace Datalium.Strata.Tests;

/// <summary>
/// Proves the Phase 1 interop constraint: a fixture written once under <c>schema/fixtures/</c>
/// deserializes into these C# types and reserializes back to the same JSON, matching the
/// equivalent round-trip tests in <c>rust/strata-core/src/schema.rs</c> and
/// <c>rust/strata-memory/src/schema.rs</c>.
/// </summary>
public class SchemaRoundTripTests
{
    private static string ReadFixture(string fileName, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "schema", "fixtures", fileName));
        return File.ReadAllText(path);
    }

    private static void AssertRoundTrips(string raw, JsonNode? roundTripped)
    {
        var original = JsonNode.Parse(raw);
        Assert.True(
            JsonNode.DeepEquals(original, roundTripped),
            $"Round-tripped JSON did not match the fixture.\nExpected: {original}\nActual: {roundTripped}");
    }

    [Fact]
    public void Checkpoint_RoundTrips_Through_The_Shared_Fixture()
    {
        var raw = ReadFixture("checkpoint.sample.json");

        var checkpoint = JsonSerializer.Deserialize<Checkpoint>(raw)!;
        var roundTripped = JsonSerializer.SerializeToNode(checkpoint);

        AssertRoundTrips(raw, roundTripped);
    }

    [Theory]
    [InlineData("checkpoint-lifecycle/nested.json")]
    [InlineData("checkpoint-lifecycle/nested-failed.json")]
    [InlineData("checkpoint-lifecycle/nested-cancelled.json")]
    public void Checkpoint_With_A_Nested_Child_RoundTrips_Through_The_Shared_Fixtures(string fixture)
    {
        var raw = ReadFixture(fixture);

        var checkpoint = JsonSerializer.Deserialize<Checkpoint>(raw)!;
        var roundTripped = JsonSerializer.SerializeToNode(checkpoint);

        Assert.NotNull(checkpoint.Nested);
        AssertRoundTrips(raw, roundTripped);
    }

    [Fact]
    public void Checkpoint_With_No_Child_Writes_No_Nested_Key()
    {
        var raw = ReadFixture("checkpoint.sample.json");

        var roundTripped = JsonSerializer.SerializeToNode(JsonSerializer.Deserialize<Checkpoint>(raw)!)!;

        Assert.False(roundTripped.AsObject().ContainsKey("nested"));
    }

    [Fact]
    public void GraphDefinition_RoundTrips_Through_The_Shared_Fixture()
    {
        var raw = ReadFixture("graph-definition.sample.json");

        var graph = JsonSerializer.Deserialize<GraphDefinition>(raw)!;
        var roundTripped = JsonSerializer.SerializeToNode(graph);

        AssertRoundTrips(raw, roundTripped);
    }

    [Fact]
    public void Hierarchical_GraphDefinition_RoundTrips_Through_The_Shared_Fixture()
    {
        // design/22-Graph-Structure-In-Definitions.md: kind, branches and a nested graph.
        var raw = ReadFixture(Path.Combine("graph-export", "hierarchy.json"));

        var graph = JsonSerializer.Deserialize<GraphDefinition>(raw)!;
        var roundTripped = JsonSerializer.SerializeToNode(graph);

        AssertRoundTrips(raw, roundTripped);
    }

    [Fact]
    public void HistoryRecord_RoundTrips_Through_The_Shared_Fixture()
    {
        var raw = ReadFixture("history-record.sample.json");

        var record = JsonSerializer.Deserialize<HistoryRecord>(raw)!;
        var roundTripped = JsonSerializer.SerializeToNode(record);

        AssertRoundTrips(raw, roundTripped);
    }
}
