using System.Collections.Immutable;
using System.Text.Json;
using Datalium.Strata.SourceGenerators;
using Datalium.Strata.Tools;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Datalium.Strata.Tests.Tools;

public class StrataToolGeneratorTests
{
    // --- End-to-end: the real generated IStrataToolProvider on SampleTools ---

    [Fact]
    public void GeneratedManifestDescribesEveryStrataToolMethod()
    {
        var manifest = JsonDocument.Parse(new SampleTools().GetToolsManifest());
        var tools = manifest.RootElement.EnumerateArray().ToList();

        Assert.Equal(2, tools.Count);

        var checkInventory = tools.Single(t => t.GetProperty("name").GetString() == "check_inventory");
        Assert.Equal("Query warehouse item quantities.", checkInventory.GetProperty("description").GetString());
        var parameters = checkInventory.GetProperty("parameters");
        Assert.Equal("object", parameters.GetProperty("type").GetString());
        Assert.Equal("string", parameters.GetProperty("properties").GetProperty("sku").GetProperty("type").GetString());
        Assert.Equal("integer", parameters.GetProperty("properties").GetProperty("warehouseId").GetProperty("type").GetString());
        var required = parameters.GetProperty("required").EnumerateArray().Select(e => e.GetString()).ToList();
        Assert.Equal(["sku", "warehouseId"], required);

        var readBalance = tools.Single(t => t.GetProperty("name").GetString() == "read_user_balance");
        var balanceRequired = readBalance.GetProperty("parameters").GetProperty("required")
            .EnumerateArray().Select(e => e.GetString()).ToList();
        // `currency` has a default value, so it must not be required — the optional-parameter
        // mitigation from STRATA-SPEC-003 section 5.
        Assert.Equal(["userId"], balanceRequired);
    }

    [Fact]
    public async Task DispatchInvokesTheOriginalMethodDirectly()
    {
        var tools = new SampleTools();
        var resultJson = await tools.DispatchToolAsync(
            "check_inventory",
            """{"sku":"A100-80G","warehouseId":7}""");

        var result = JsonDocument.Parse(resultJson).RootElement.GetProperty("result").GetString();
        Assert.Equal("SKU A100-80G count is 42 in Zone 7", result);
    }

    [Fact]
    public async Task DispatchAppliesTheDefaultValueWhenAnOptionalArgumentIsOmitted()
    {
        var tools = new SampleTools();
        var resultJson = await tools.DispatchToolAsync("read_user_balance", """{"userId":7}""");
        var result = JsonDocument.Parse(resultJson).RootElement.GetProperty("result").GetString();
        Assert.Equal("User 7 balance in USD", result);
    }

    [Fact]
    public async Task DispatchHonorsAnExplicitlyProvidedOptionalArgument()
    {
        var tools = new SampleTools();
        var resultJson = await tools.DispatchToolAsync("read_user_balance", """{"userId":7,"currency":"EUR"}""");
        var result = JsonDocument.Parse(resultJson).RootElement.GetProperty("result").GetString();
        Assert.Equal("User 7 balance in EUR", result);
    }

    [Fact]
    public async Task DispatchingAnUnknownToolNameThrows()
    {
        var tools = new SampleTools();
        var ex = await Assert.ThrowsAsync<ToolNotFoundException>(
            () => tools.DispatchToolAsync("does_not_exist", "{}"));
        Assert.Equal("does_not_exist", ex.ToolName);
    }

    // --- STRATA-SPEC-003 §2: zero runtime reflection in the dispatch path ---

    [Fact]
    public void GeneratedDispatchCodeNeverUsesReflection()
    {
        var generatedSource = RunGenerator(
            """
            using Datalium.Strata.Tools;

            namespace ReflectionCheckNs;

            public partial class ReflectionCheckTools
            {
                [StrataTool(Name = "ping", Description = "ping")]
                public static string Ping(string message) => message;
            }
            """).generatedSource;

        Assert.DoesNotContain("System.Reflection", generatedSource);
        Assert.DoesNotContain("GetMethod(", generatedSource);
        Assert.DoesNotContain("MethodInfo", generatedSource);
        Assert.DoesNotContain("Invoke(", generatedSource);
    }

    // --- STRATA-SPEC-003 schema-drift regression: bad signatures fail at compile time ---

    [Fact]
    public void NonPartialContainingClassProducesAnAnalyzerError()
    {
        var diagnostics = RunGenerator(
            """
            using Datalium.Strata.Tools;

            namespace BadNs;

            public class NotPartial
            {
                [StrataTool(Name = "bad", Description = "bad")]
                public static string DoThing() => "x";
            }
            """).diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "STRATA001");
    }

    [Fact]
    public void UnsupportedParameterTypeProducesAnAnalyzerError()
    {
        var diagnostics = RunGenerator(
            """
            using Datalium.Strata.Tools;

            namespace BadNs;

            public partial class BadTools
            {
                [StrataTool(Name = "bad", Description = "bad")]
                public static string DoThing(object anything) => "x";
            }
            """).diagnostics;

        Assert.Contains(diagnostics, d => d.Id == "STRATA002");
    }

    [Fact]
    public void ADuplicateToolNameInOneClassProducesAnAnalyzerErrorAndNoDuplicateCase()
    {
        var (generated, diagnostics) = RunGenerator(
            """
            using Datalium.Strata.Tools;

            namespace BadNs;

            public partial class TwinTools
            {
                [StrataTool(Name = "twin", Description = "first")]
                public static string First() => "1";

                [StrataTool(Name = "twin", Description = "second")]
                public static string Second() => "2";
            }
            """);

        var duplicate = Assert.Single(diagnostics, d => d.Id == "STRATA003");
        Assert.Contains("'Second'", duplicate.GetMessage());
        Assert.Contains("'First'", duplicate.GetMessage());
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(generated, "case \"twin\":"));
    }

    [Fact]
    public void ContextAndTokenParametersAreLeftOutOfTheManifestAndTheArgumentsRecord()
    {
        var (generated, diagnostics) = RunGenerator(
            """
            using System.Threading;
            using Datalium.Strata.Tools;

            namespace GoodNs;

            public partial class AwareTools
            {
                [StrataTool(Name = "aware", Description = "aware")]
                public static string Aware(ToolCallContext context, string sku, CancellationToken ct) => sku;

                [StrataTool(Name = "only_context", Description = "only context")]
                public static string OnlyContext(ToolCallContext context) => "x";
            }
            """);

        Assert.DoesNotContain(diagnostics, d => d.Severity == DiagnosticSeverity.Error);
        Assert.Contains("""\"properties\":{\"sku\":{\"type\":\"string\"}},\"required\":[\"sku\"]""", generated);
        Assert.Contains("""\"name\":\"only_context\",\"description\":\"only context\",\"parameters\":{\"type\":\"object\",\"properties\":{},\"required\":[]}""", generated);
        Assert.Contains("private sealed record AwareStrataArgs(string sku);", generated);
        Assert.DoesNotContain("OnlyContextStrataArgs", generated);
        Assert.Contains("Aware(context, args.sku, cancellationToken)", generated);
        Assert.Contains("OnlyContext(context)", generated);
    }

    // --- SG-33: optional nullable parameters (design/23-Tool-Middleware.md) ---

    [Theory]
    [InlineData("string? app", "string? app = null")]
    [InlineData("string? app = \"web\"", "string? app = \"web\"")]
    [InlineData("int? limit", "int? limit = null")]
    public void AnOptionalNullableParameterGeneratesCodeThatCompilesWithoutWarnings(string parameter, string recordParameter)
    {
        var (generated, diagnostics) = CompileWithGenerator(
            $$"""
            using Datalium.Strata.Tools;

            namespace OptionalNs;

            public partial class OptionalTools
            {
                [StrataTool(Name = "find", Description = "find")]
                public static string Find({{parameter}}) => "x";
            }
            """);

        // The Roslyn 4.11 this test drives also warns that the generated async dispatch has no await
        // when every tool is synchronous. The SDK's compiler doesn't, and that warning predates SG-33.
        Assert.Empty(diagnostics.Where(d => d.Severity >= DiagnosticSeverity.Warning && d.Id != "CS1998"));
        Assert.Contains($"private sealed record FindStrataArgs({recordParameter});", generated);
        Assert.Contains("""\"required\":[]""", generated);
    }

    [Fact]
    public async Task AnOptionalStringParameterIsNullWhenOmittedAndPassedWhenGiven()
    {
        var tools = new OptionalTools();

        Assert.Equal("app=(none) region=eu limit=(none)", Result(await tools.DispatchToolAsync("find_service", "{}")));
        Assert.Equal(
            "app=web region=us limit=3",
            Result(await tools.DispatchToolAsync("find_service", """{"app":"web","region":"us","limit":3}""")));
        var required = JsonDocument.Parse(tools.GetToolsManifest()).RootElement[0]
            .GetProperty("parameters").GetProperty("required");
        Assert.Equal(0, required.GetArrayLength());
    }

    private static string Result(string json) => JsonDocument.Parse(json).RootElement.GetProperty("result").GetString()!;

    /// <summary>
    /// Runs the generator over <paramref name="source"/> with nullable reference types enabled, then
    /// compiles the result, returning the generated source and every diagnostic of the compilation.
    /// </summary>
    private static (string generatedSource, ImmutableArray<Diagnostic> diagnostics) CompileWithGenerator(string source)
    {
        var compilation = CSharpCompilation.Create(
            "StrataToolGeneratorNullableTestAssembly",
            [CSharpSyntaxTree.ParseText(source)],
            References(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new StrataToolGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var generatorDiagnostics);

        var generatedSource = string.Join(
            Environment.NewLine,
            driver.GetRunResult().GeneratedTrees.Select(tree => tree.ToString()));
        return (generatedSource, generatorDiagnostics.AddRange(output.GetDiagnostics()));
    }

    private static MetadataReference[] References() =>
        ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Select(path => (MetadataReference)MetadataReference.CreateFromFile(path))
            .Append(MetadataReference.CreateFromFile(typeof(StrataToolAttribute).Assembly.Location))
            .ToArray();

    /// <summary>
    /// Drives <see cref="StrataToolGenerator"/> directly (not through the test project's own
    /// analyzer wiring) against an isolated in-memory compilation, so a deliberately-broken
    /// fixture can be asserted on without breaking this project's own build.
    /// </summary>
    private static (string generatedSource, ImmutableArray<Diagnostic> diagnostics) RunGenerator(string source)
    {
        var trustedAssembliesPaths = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator);
        var references = trustedAssembliesPaths
            .Select(path => (MetadataReference)MetadataReference.CreateFromFile(path))
            .Append(MetadataReference.CreateFromFile(typeof(StrataToolAttribute).Assembly.Location))
            .ToArray();

        var compilation = CSharpCompilation.Create(
            "StrataToolGeneratorTestAssembly",
            [CSharpSyntaxTree.ParseText(source)],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new StrataToolGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out _, out var diagnostics);

        var runResult = driver.GetRunResult();
        var generatedSource = string.Join(
            Environment.NewLine,
            runResult.GeneratedTrees.Select(tree => tree.ToString()));

        return (generatedSource, diagnostics);
    }
}
