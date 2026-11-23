using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Mcp;
using Datalium.Strata.Tests.Backends;

namespace Datalium.Strata.Tests.Mcp;

/// <summary>
/// The shared MCP fixtures in <c>schema/fixtures/mcp/</c>, which Rust's <c>datalium-strata-mcp</c> replays too
/// (<c>design/50-MCP-Client.md</c>, "Wire format"): the result text, the header encoding, the <c>x-mcp-header</c>
/// rules, SSE reading and the era rule.
/// </summary>
public sealed class McpFixtureTests
{
    private static JsonArray Cases(string file) => CheckpointQueryContractTests.Fixture(Path.Combine("mcp", file))["cases"]!.AsArray();

    private static TheoryData<string> Names(string file)
    {
        var names = new TheoryData<string>();
        foreach (var @case in Cases(file))
        {
            names.Add(@case!["name"]!.GetValue<string>());
        }

        return names;
    }

    private static JsonNode Case(string file, string name) => Cases(file).Single(c => c!["name"]!.GetValue<string>() == name)!;

    private static JsonElement Element(JsonNode? node) => JsonDocument.Parse(node?.ToJsonString() ?? "null").RootElement.Clone();

    public static TheoryData<string> ResultCases() => Names("tool-results.json");

    [Theory]
    [MemberData(nameof(ResultCases))]
    public void A_Result_Becomes_The_Text_Both_Engines_Return(string name)
    {
        var @case = Case("tool-results.json", name);
        var text = McpResults.Read(Element(@case["result"])).ToText();
        var expected = @case["text"]!.GetValue<string>();
        if (@case["json"]?.GetValue<bool>() == true)
        {
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(expected), JsonNode.Parse(text)), $"expected {expected}, got {text}");
        }
        else
        {
            Assert.Equal(expected, text);
        }
    }

    public static TheoryData<string> HeaderValueCases() => Names("header-values.json");

    [Theory]
    [MemberData(nameof(HeaderValueCases))]
    public void A_Value_Becomes_Its_Header_Encoding(string name)
    {
        var @case = Case("header-values.json", name);
        Assert.Equal(@case["header"]?.GetValue<string>(), McpHeaders.Value(Element(@case["value"])));
    }

    public static TheoryData<string> AnnotationCases() => Names("x-mcp-header.json");

    [Theory]
    [MemberData(nameof(AnnotationCases))]
    public void A_Tools_Header_Annotations_Keep_Or_Reject_It_And_Name_Its_Headers(string name)
    {
        var @case = Case("x-mcp-header.json", name);
        var (parameters, rejected) = McpHeaders.Read(Element(@case["inputSchema"]));
        Assert.Equal(@case["rejected"]?.GetValue<string>(), rejected);
        foreach (var call in @case["calls"]?.AsArray() ?? [])
        {
            var headers = McpHeaders.For(parameters, Element(call!["arguments"])).ToDictionary(h => h.Key, h => h.Value);
            var expected = call["headers"]!.AsObject().ToDictionary(h => h.Key, h => h.Value!.GetValue<string>());
            Assert.Equal(expected.OrderBy(h => h.Key), headers.OrderBy(h => h.Key));
        }
    }

    public static TheoryData<string> SseCases() => Names("sse.json");

    [Theory]
    [MemberData(nameof(SseCases))]
    public async Task An_Event_Stream_Becomes_Events(string name)
    {
        var @case = Case("sse.json", name);
        using var input = new MemoryStream(Encoding.UTF8.GetBytes(@case["input"]!.GetValue<string>()));
        var events = new List<(string Type, string Data, string? Id)>();
        await foreach (var item in McpSse.ReadAsync(input))
        {
            events.Add((item.EventType, item.Data, item.LastEventId));
        }

        var expected = @case["events"]!.AsArray()
            .Select(e => (e!["type"]!.GetValue<string>(), e["data"]!.GetValue<string>(), e["id"]?.GetValue<string>()))
            .ToList();
        Assert.Equal(expected, events);
    }

    public static TheoryData<string> EraCases() => Names("era-detection.json");

    [Theory]
    [MemberData(nameof(EraCases))]
    public async Task A_Probe_Answer_Decides_The_Era(string name)
    {
        var @case = Case("era-detection.json", name);
        var answer = @case["answer"]!;
        EraOutcome outcome;
        if (@case["transport"]!.GetValue<string>() == "stdio")
        {
            var response = answer["silence"] is not null
                ? null
                : RpcResponse.From(Element(new JsonObject { ["jsonrpc"] = "2.0", ["id"] = 1, [answer["result"] is not null ? "result" : "error"] = (answer["result"] ?? answer["error"])!.DeepClone() }));
            outcome = McpProtocol.Classify(response, http: false);
        }
        else
        {
            var body = answer["body"]!.GetValue<string>();
            RpcResponse? message;
            if (answer["contentType"]!.GetValue<string>() == "text/event-stream")
            {
                message = null;
                await foreach (var item in McpSse.ReadAsync(new MemoryStream(Encoding.UTF8.GetBytes(body))))
                {
                    message = RpcResponse.Parse(item.Data) ?? message;
                }
            }
            else
            {
                message = body.Length == 0 ? null : RpcResponse.Parse(body);
            }

            outcome = McpProtocol.ClassifyHttp(answer["status"]!.GetValue<int>(), message);
        }

        var expected = @case["outcome"]!;
        Assert.Equal(expected["era"]!.GetValue<string>(), outcome.Era);
        if (expected["version"] is { } version)
        {
            Assert.Equal(version.GetValue<string>(), outcome.Version);
        }

        foreach (var word in expected["contains"]?.AsArray() ?? [])
        {
            Assert.Contains(word!.GetValue<string>(), outcome.Error, StringComparison.Ordinal);
        }
    }
}
