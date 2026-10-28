using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Tests.Scheduling;
using Datalium.Strata.Tools;

namespace Datalium.Strata.Tests.Tools;

/// <summary>
/// Arguments a tool can't read (<c>design/20-Tool-Dispatch-And-Registry.md</c>): the shared words in
/// <c>schema/fixtures/tools/argument-problems.json</c>, which <c>rust/strata-core/tests/argument_problems.rs</c>
/// replays too, and the generated dispatch that throws them as a <see cref="ToolArgumentsException"/>.
/// </summary>
public sealed class ToolArgumentProblemTests
{
    private static JsonArray Cases() =>
        RunSchedulerTests.Fixture(Path.Combine("tools", "argument-problems.json"))["cases"]!.AsArray();

    public static TheoryData<string> CaseNames()
    {
        var names = new TheoryData<string>();
        foreach (var @case in Cases())
        {
            names.Add(@case!["name"]!.GetValue<string>());
        }

        return names;
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public void Shared_Case(string name)
    {
        var @case = Cases().Single(c => c!["name"]!.GetValue<string>() == name)!;

        var problems = ToolArguments.Problems(@case["arguments"]!.GetValue<string>(), @case["parameters"]!.ToJsonString());

        Assert.Equal(@case["problems"]?.GetValue<string>(), problems);
    }

    private static async Task<ToolArgumentsException> Refused(IStrataToolProvider tools, string tool, string arguments)
    {
        var refused = await Assert.ThrowsAsync<ToolArgumentsException>(() => tools.DispatchToolAsync(tool, arguments));
        Assert.Equal(tool, refused.ToolName);
        Assert.Equal($"Invalid arguments for tool '{tool}': {refused.Problems}", refused.Message);
        return refused;
    }

    [Fact]
    public async Task A_Missing_Required_Argument_Is_Refused_Before_The_Tool_Runs()
    {
        var refused = await Refused(new SampleTools(), "check_inventory", """{"sku":"A100"}""");

        Assert.Equal("argument 'warehouseId' is missing", refused.Problems);
        Assert.IsType<JsonException>(refused.InnerException);
    }

    [Fact]
    public async Task A_Wrong_Type_Is_Named_Without_The_Generated_Record()
    {
        var refused = await Refused(new SampleTools(), "check_inventory", """{"sku":42,"warehouseId":"seven"}""");

        Assert.Equal("argument 'sku' must be a string; argument 'warehouseId' must be an integer", refused.Problems);
        Assert.DoesNotContain("StrataArgs", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Null_For_A_Required_Argument_Is_Refused()
    {
        var refused = await Refused(new SampleTools(), "check_inventory", """{"sku":null,"warehouseId":7}""");

        Assert.Equal("argument 'sku' must be a string", refused.Problems);
    }

    [Fact]
    public async Task Null_For_An_Optional_Argument_That_Isnt_Nullable_Is_Refused_By_Name()
    {
        // `string currency = "USD"`: optional, so the shared rule passes null, but the parameter can't hold it.
        var refused = await Refused(new SampleTools(), "read_user_balance", """{"userId":7,"currency":null}""");

        Assert.Equal("argument 'currency' can't be null", refused.Problems);
    }

    [Fact]
    public async Task An_Argument_The_Rule_Doesnt_Explain_Is_Named_By_The_Property_The_Parser_Stopped_At()
    {
        // An integer, but too large for an int.
        var refused = await Refused(new SampleTools(), "check_inventory", """{"sku":"A100","warehouseId":5000000000}""");

        Assert.Equal("argument 'warehouseId' isn't valid", refused.Problems);
    }

    [Theory]
    [InlineData("null", "the arguments aren't a JSON object")]
    [InlineData("[1]", "the arguments aren't a JSON object")]
    [InlineData("""{"sku":""", "the arguments aren't valid JSON")]
    public async Task Arguments_That_Arent_An_Object_Are_Refused(string arguments, string problems)
    {
        var refused = await Refused(new SampleTools(), "check_inventory", arguments);

        Assert.Equal(problems, refused.Problems);
    }

    [Fact]
    public async Task Optional_Nullable_Arguments_May_Still_Be_Null_Or_Absent()
    {
        var result = await new OptionalTools().DispatchToolAsync("find_service", """{"region":null}""");

        Assert.Equal("""{"result":"app=(none) region=(none) limit=(none)"}""", result);
    }

    [Fact]
    public async Task A_Tools_Own_ArgumentException_Is_Not_An_Arguments_Refusal()
    {
        var thrown = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => new StrictTools().DispatchToolAsync("refund", """{"amount":-5}"""));

        Assert.Equal("amount", thrown.ParamName);
    }
}

/// <summary>A tool whose own code throws <see cref="ArgumentOutOfRangeException"/>.</summary>
public partial class StrictTools
{
    [StrataTool(Name = "refund", Description = "Refunds an amount.")]
    public static string Refund(int amount) =>
        amount > 0 ? $"refunded {amount}" : throw new ArgumentOutOfRangeException(nameof(amount), "a refund must be positive");
}
