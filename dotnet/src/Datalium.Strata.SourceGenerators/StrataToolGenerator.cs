using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Datalium.Strata.SourceGenerators;

/// <summary>
/// Implements the C# half of STRATA-SPEC-003: discovers
/// <c>[StrataTool]</c>-annotated methods and, for each <c>partial class</c>
/// declaring one or more, emits a matching <c>IStrataToolProvider</c>
/// implementation — a compile-time JSON-Schema manifest string plus a static
/// <c>switch</c> dispatching directly to the annotated methods. No
/// <c>System.Reflection</c> API is used anywhere in the emitted code: every
/// dispatch case names its target method literally.
///
/// Matches attributes by full metadata name only (<see cref="StrataToolAttributeFullName"/>),
/// so this generator project intentionally has no reference to
/// <c>Datalium.Strata</c> (the assembly that defines that attribute) — avoiding
/// a circular project reference between the runtime library and its own analyzer.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class StrataToolGenerator : IIncrementalGenerator
{
    private const string StrataToolAttributeFullName = "Datalium.Strata.Tools.StrataToolAttribute";

    private const string ToolCallContextFullName = "Datalium.Strata.Tools.ToolCallContext";

    private const string CancellationTokenFullName = "System.Threading.CancellationToken";

    /// <summary>
    /// How a JSON parameter's type is written into the arguments record: fully qualified, keeping the
    /// <c>?</c> of a nullable reference type, so an optional <c>string?</c> becomes <c>string? x = null</c>
    /// under <c>#nullable enable</c> rather than <c>string x = null</c> (CS8625; SG-33,
    /// <c>design/23-Tool-Middleware.md</c>).
    /// </summary>
    private static readonly SymbolDisplayFormat ArgumentTypeFormat = SymbolDisplayFormat.FullyQualifiedFormat
        .AddMiscellaneousOptions(SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    private static readonly DiagnosticDescriptor ContainingClassMustBePartial = new(
        id: "STRATA001",
        title: "[StrataTool]-annotated methods must live in a partial class",
        messageFormat: "Class '{0}' declares a [StrataTool] method but is not declared 'partial'; " +
                        "Datalium.Strata.SourceGenerators cannot add the generated IStrataToolProvider implementation",
        category: "Datalium.Strata.Tools",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor UnsupportedParameterType = new(
        id: "STRATA002",
        title: "Unsupported [StrataTool] parameter type",
        messageFormat: "Parameter '{0}' of tool method '{1}' has type '{2}', which cannot be mapped to a JSON-Schema " +
                        "type; supported types are string, bool, and the built-in numeric types " +
                        "(optionally nullable / with a default value), plus ToolCallContext and CancellationToken, " +
                        "which receive the call's context and token and are not part of the manifest",
        category: "Datalium.Strata.Tools",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor DuplicateToolName = new(
        id: "STRATA003",
        title: "Two [StrataTool] methods in one class share a tool name",
        messageFormat: "Tool name '{0}' of method '{1}' is already used by method '{2}' in class '{3}'; " +
                        "tool names must be unique within a provider",
        category: "Datalium.Strata.Tools",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var extracted = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                StrataToolAttributeFullName,
                predicate: static (node, _) => node is MethodDeclarationSyntax,
                transform: static (ctx, _) => Extract(ctx))
            .Collect();

        context.RegisterSourceOutput(extracted, static (spc, results) => EmitAll(spc, results));
    }

    private static void EmitAll(SourceProductionContext spc, ImmutableArray<ExtractResult> results)
    {
        foreach (var result in results)
        {
            foreach (var diagnostic in result.Diagnostics)
            {
                spc.ReportDiagnostic(diagnostic);
            }
        }

        var validTools = results
            .Where(static r => r.Info is not null)
            .Select(static r => r.Info!.Value);

        foreach (var group in validTools.GroupBy(static i => (i.Namespace, i.ClassName)))
        {
            // The first method to claim a name keeps it; a duplicate is reported and left out, so the
            // generated switch has no duplicate case label to report on top of it.
            var claimed = new Dictionary<string, ToolMethodInfo>();
            var unique = ImmutableArray.CreateBuilder<ToolMethodInfo>();
            foreach (var tool in group)
            {
                if (claimed.TryGetValue(tool.ToolName, out var first))
                {
                    spc.ReportDiagnostic(Diagnostic.Create(
                        DuplicateToolName, tool.Location, tool.ToolName, tool.MethodName, first.MethodName, tool.ClassName));
                    continue;
                }

                claimed.Add(tool.ToolName, tool);
                unique.Add(tool);
            }

            var tools = unique.ToImmutable();
            var source = EmitProvider(group.Key.Namespace, group.Key.ClassName, tools);
            var hintName = $"{(group.Key.Namespace is null ? string.Empty : group.Key.Namespace + ".")}{group.Key.ClassName}.StrataTools.g.cs";
            spc.AddSource(hintName, source);
        }
    }

    private static ExtractResult Extract(GeneratorAttributeSyntaxContext ctx)
    {
        var methodSymbol = (IMethodSymbol)ctx.TargetSymbol;
        var methodSyntax = (MethodDeclarationSyntax)ctx.TargetNode;
        var classSymbol = methodSymbol.ContainingType;
        var classDeclaration = methodSyntax.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        var diagnostics = ImmutableArray.CreateBuilder<Diagnostic>();

        var isPartial = classDeclaration?.Modifiers.Any(SyntaxKind.PartialKeyword) ?? false;
        if (!isPartial)
        {
            diagnostics.Add(Diagnostic.Create(
                ContainingClassMustBePartial,
                classDeclaration?.Identifier.GetLocation() ?? methodSyntax.GetLocation(),
                classSymbol.Name));
            return new ExtractResult(null, diagnostics.ToImmutable());
        }

        var attribute = methodSymbol.GetAttributes()
            .First(a => a.AttributeClass?.ToDisplayString() == StrataToolAttributeFullName);

        var toolName = methodSymbol.Name;
        var description = string.Empty;
        foreach (var namedArg in attribute.NamedArguments)
        {
            if (namedArg is { Key: "Name", Value.Value: string n })
            {
                toolName = n;
            }
            else if (namedArg is { Key: "Description", Value.Value: string d })
            {
                description = d;
            }
        }

        var parameters = ImmutableArray.CreateBuilder<ParameterInfo>();
        var hasUnsupportedParameter = false;
        foreach (var parameter in methodSymbol.Parameters)
        {
            var typeName = parameter.Type.ToDisplayString();
            if (typeName is ToolCallContextFullName or CancellationTokenFullName)
            {
                parameters.Add(new ParameterInfo(
                    parameter.Name,
                    parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    JsonType: string.Empty,
                    Optional: true,
                    DefaultLiteral: "default",
                    Kind: typeName == ToolCallContextFullName ? ParameterKind.Context : ParameterKind.Token));
                continue;
            }

            var underlyingType = UnwrapNullableValueType(parameter.Type, out var isNullableValueType);
            var jsonType = MapType(underlyingType);
            if (jsonType is null)
            {
                diagnostics.Add(Diagnostic.Create(
                    UnsupportedParameterType,
                    parameter.Locations.FirstOrDefault() ?? methodSyntax.GetLocation(),
                    parameter.Name,
                    methodSymbol.Name,
                    parameter.Type.ToDisplayString()));
                hasUnsupportedParameter = true;
                continue;
            }

            var optional = parameter.HasExplicitDefaultValue
                || parameter.Type.NullableAnnotation == NullableAnnotation.Annotated
                || isNullableValueType;

            var defaultLiteral = parameter.HasExplicitDefaultValue
                ? SymbolDisplay.FormatPrimitive(parameter.ExplicitDefaultValue!, quoteStrings: true, useHexadecimalNumbers: false)
                : "null";

            parameters.Add(new ParameterInfo(
                parameter.Name,
                parameter.Type.ToDisplayString(ArgumentTypeFormat),
                jsonType,
                optional,
                defaultLiteral,
                ParameterKind.Json));
        }

        if (hasUnsupportedParameter)
        {
            return new ExtractResult(null, diagnostics.ToImmutable());
        }

        var (isAwaited, hasReturnValue) = AnalyzeReturnType(methodSymbol.ReturnType);

        var info = new ToolMethodInfo(
            classSymbol.ContainingNamespace.IsGlobalNamespace ? null : classSymbol.ContainingNamespace.ToDisplayString(),
            classSymbol.Name,
            methodSymbol.Name,
            toolName,
            description,
            isAwaited,
            hasReturnValue,
            parameters.ToImmutable(),
            methodSyntax.Identifier.GetLocation());

        return new ExtractResult(info, diagnostics.ToImmutable());
    }

    private static ITypeSymbol UnwrapNullableValueType(ITypeSymbol type, out bool wasNullableValueType)
    {
        if (type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T } named)
        {
            wasNullableValueType = true;
            return named.TypeArguments[0];
        }

        wasNullableValueType = false;
        return type;
    }

    private static string? MapType(ITypeSymbol type) => type.SpecialType switch
    {
        SpecialType.System_String => "string",
        SpecialType.System_Boolean => "boolean",
        SpecialType.System_SByte or SpecialType.System_Byte or
            SpecialType.System_Int16 or SpecialType.System_UInt16 or
            SpecialType.System_Int32 or SpecialType.System_UInt32 or
            SpecialType.System_Int64 or SpecialType.System_UInt64 => "integer",
        SpecialType.System_Single or SpecialType.System_Double or SpecialType.System_Decimal => "number",
        _ => null,
    };

    private static (bool IsAwaited, bool HasReturnValue) AnalyzeReturnType(ITypeSymbol returnType)
    {
        if (returnType.SpecialType == SpecialType.System_Void)
        {
            return (false, false);
        }

        if (returnType is INamedTypeSymbol named)
        {
            var originalName = named.OriginalDefinition.ToDisplayString();
            switch (originalName)
            {
                case "System.Threading.Tasks.Task":
                case "System.Threading.Tasks.ValueTask":
                    return (true, false);
                case "System.Threading.Tasks.Task<TResult>":
                case "System.Threading.Tasks.ValueTask<TResult>":
                    return (true, true);
            }
        }

        return (false, true);
    }

    private static string EmitProvider(string? ns, string className, ImmutableArray<ToolMethodInfo> tools)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        if (ns is not null)
        {
            sb.AppendLine($"namespace {ns};");
            sb.AppendLine();
        }

        sb.AppendLine($"partial class {className} : global::Datalium.Strata.Tools.IStrataToolProvider");
        sb.AppendLine("{");

        var manifestLiteral = SymbolDisplay.FormatLiteral(BuildManifestJson(tools), quote: true);
        sb.AppendLine($"    public string GetToolsManifest() => {manifestLiteral};");
        sb.AppendLine();

        sb.AppendLine("    public global::System.Threading.Tasks.Task<string> DispatchToolAsync(string toolName, string jsonArguments) =>");
        sb.AppendLine("        DispatchToolAsync(toolName, jsonArguments, global::Datalium.Strata.Tools.ToolCallContext.None, default);");
        sb.AppendLine();
        sb.AppendLine("    public async global::System.Threading.Tasks.Task<string> DispatchToolAsync(");
        sb.AppendLine("        string toolName,");
        sb.AppendLine("        string jsonArguments,");
        sb.AppendLine("        global::Datalium.Strata.Tools.ToolCallContext context,");
        sb.AppendLine("        global::System.Threading.CancellationToken cancellationToken = default)");
        sb.AppendLine("    {");
        sb.AppendLine("        switch (toolName)");
        sb.AppendLine("        {");
        foreach (var tool in tools)
        {
            var argsRecordName = tool.MethodName + "StrataArgs";
            sb.AppendLine($"            case {SymbolDisplay.FormatLiteral(tool.ToolName, quote: true)}:");
            sb.AppendLine("            {");
            if (tool.JsonParameters.Any())
            {
                // Arguments that don't fit are the caller's mistake, not the tool's failure: a ToolArgumentsException
                // in the words both engines use (design/20, "Arguments a tool can't read"), so an agent can tell the
                // model what to fix and a tool's own ArgumentException or JsonException stays the tool's.
                var toolNameLiteral = SymbolDisplay.FormatLiteral(tool.ToolName, quote: true);
                sb.AppendLine($"                {argsRecordName} args;");
                sb.AppendLine("                try");
                sb.AppendLine("                {");
                sb.AppendLine($"                    args = global::System.Text.Json.JsonSerializer.Deserialize<{argsRecordName}>(jsonArguments, StrataToolArgumentOptions)");
                sb.AppendLine($"                        ?? throw new global::Datalium.Strata.Tools.ToolArgumentsException({toolNameLiteral}, \"the arguments aren't a JSON object\");");
                sb.AppendLine("                }");
                sb.AppendLine("                catch (global::System.Text.Json.JsonException ex)");
                sb.AppendLine("                {");
                sb.AppendLine($"                    throw global::Datalium.Strata.Tools.ToolArgumentsException.From({toolNameLiteral}, jsonArguments, {SymbolDisplay.FormatLiteral(BuildParametersJson(tool), quote: true)}, ex);");
                sb.AppendLine("                }");
            }

            var callArgs = string.Join(", ", tool.Parameters.Select(static p => p.Kind switch
            {
                ParameterKind.Context => "context",
                ParameterKind.Token => "cancellationToken",
                _ => $"args.{p.Name}",
            }));
            var call = $"{tool.MethodName}({callArgs})";
            if (tool.IsAwaited && tool.HasReturnValue)
            {
                sb.AppendLine($"                var result = await {call};");
                sb.AppendLine("                return global::System.Text.Json.JsonSerializer.Serialize(new { result });");
            }
            else if (tool.IsAwaited)
            {
                sb.AppendLine($"                await {call};");
                sb.AppendLine("                return \"{}\";");
            }
            else if (tool.HasReturnValue)
            {
                sb.AppendLine($"                var result = {call};");
                sb.AppendLine("                return global::System.Text.Json.JsonSerializer.Serialize(new { result });");
            }
            else
            {
                sb.AppendLine($"                {call};");
                sb.AppendLine("                return \"{}\";");
            }

            sb.AppendLine("            }");
        }

        sb.AppendLine("            default:");
        sb.AppendLine("                throw new global::Datalium.Strata.Tools.ToolNotFoundException(toolName);");
        sb.AppendLine("        }");
        sb.AppendLine("    }");

        if (tools.Any(static t => t.JsonParameters.Any()))
        {
            // A required parameter (no default, not nullable) must be present and a non-nullable one not null, as the
            // manifest says and as Rust's serde reads a #[strata_tool]'s arguments.
            sb.AppendLine();
            sb.AppendLine("    private static readonly global::System.Text.Json.JsonSerializerOptions StrataToolArgumentOptions = new()");
            sb.AppendLine("    {");
            sb.AppendLine("        RespectNullableAnnotations = true,");
            sb.AppendLine("        RespectRequiredConstructorParameters = true,");
            sb.AppendLine("    };");
        }

        foreach (var tool in tools.Where(static t => t.JsonParameters.Any()))
        {
            var argsRecordName = tool.MethodName + "StrataArgs";
            var ctorParams = string.Join(", ", tool.JsonParameters.Select(static p =>
                p.Optional
                    ? $"{p.FullyQualifiedTypeName} {p.Name} = {p.DefaultLiteral}"
                    : $"{p.FullyQualifiedTypeName} {p.Name}"));
            sb.AppendLine();
            sb.AppendLine($"    private sealed record {argsRecordName}({ctorParams});");
        }

        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string BuildManifestJson(ImmutableArray<ToolMethodInfo> tools)
    {
        var sb = new StringBuilder();
        sb.Append('[');
        for (var i = 0; i < tools.Length; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }

            var tool = tools[i];
            sb.Append('{');
            sb.Append("\"name\":").Append(JsonString(tool.ToolName)).Append(',');
            sb.Append("\"description\":").Append(JsonString(tool.Description)).Append(',');
            sb.Append("\"parameters\":").Append(BuildParametersJson(tool));
            sb.Append('}');
        }

        sb.Append(']');
        return sb.ToString();
    }

    /// <summary>A tool's <c>parameters</c> schema, as its manifest entry has it.</summary>
    private static string BuildParametersJson(ToolMethodInfo tool)
    {
        var sb = new StringBuilder();
        sb.Append("{\"type\":\"object\",\"properties\":{");
        var jsonParameters = tool.JsonParameters.ToArray();
        for (var p = 0; p < jsonParameters.Length; p++)
        {
            if (p > 0)
            {
                sb.Append(',');
            }

            var param = jsonParameters[p];
            sb.Append(JsonString(param.Name)).Append(":{\"type\":").Append(JsonString(param.JsonType)).Append('}');
        }

        sb.Append("},\"required\":[");
        var required = jsonParameters.Where(static p => !p.Optional).Select(static p => p.Name).ToArray();
        for (var r = 0; r < required.Length; r++)
        {
            if (r > 0)
            {
                sb.Append(',');
            }

            sb.Append(JsonString(required[r]));
        }

        sb.Append("]}");
        return sb.ToString();
    }

    private static string JsonString(string value)
    {
        var sb = new StringBuilder();
        sb.Append('"');
        foreach (var c in value)
        {
            switch (c)
            {
                case '"':
                    sb.Append("\\\"");
                    break;
                case '\\':
                    sb.Append("\\\\");
                    break;
                case '\n':
                    sb.Append("\\n");
                    break;
                case '\r':
                    sb.Append("\\r");
                    break;
                case '\t':
                    sb.Append("\\t");
                    break;
                default:
                    if (c < 0x20)
                    {
                        sb.Append("\\u").Append(((int)c).ToString("x4"));
                    }
                    else
                    {
                        sb.Append(c);
                    }

                    break;
            }
        }

        sb.Append('"');
        return sb.ToString();
    }

    /// <summary>
    /// What a tool method's parameter receives: a JSON argument (in the manifest), or the call's
    /// <c>ToolCallContext</c> / <c>CancellationToken</c> (not in the manifest).
    /// </summary>
    private enum ParameterKind
    {
        Json,
        Context,
        Token,
    }

    private readonly record struct ParameterInfo(
        string Name,
        string FullyQualifiedTypeName,
        string JsonType,
        bool Optional,
        string DefaultLiteral,
        ParameterKind Kind);

    private readonly record struct ToolMethodInfo(
        string? Namespace,
        string ClassName,
        string MethodName,
        string ToolName,
        string Description,
        bool IsAwaited,
        bool HasReturnValue,
        ImmutableArray<ParameterInfo> Parameters,
        Location Location)
    {
        public IEnumerable<ParameterInfo> JsonParameters => Parameters.Where(static p => p.Kind == ParameterKind.Json);
    }

    private readonly record struct ExtractResult(ToolMethodInfo? Info, ImmutableArray<Diagnostic> Diagnostics);
}
