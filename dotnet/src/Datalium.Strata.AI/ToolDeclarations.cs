using System.Text;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.AI;

/// <summary>
/// A registry's tools as a model sees them, and the way back (<c>design/44-LLM-Integration.md</c>, "Tool names").
/// Mirrors the Rust <c>ToolDeclarations</c>.
/// </summary>
/// <remarks>
/// Providers accept tool names matching <c>^[a-zA-Z0-9_-]{1,64}$</c>, and Strata's are free text
/// (<c>k8s.deployment</c>). Each other character becomes <c>_</c>, the name is cut to 64 characters, and a name already
/// taken gets <c>_2</c>, <c>_3</c>, … (cut to fit), in registration order: <c>k8s.deployment</c> is
/// <c>k8s_deployment</c>. The declarations are <see cref="AIFunctionFactory.CreateDeclaration"/>s, never invocable
/// functions, so a client that invokes functions itself hands the calls back. Built once per agent, so the model sees
/// the same names on every turn.
/// </remarks>
public sealed class ToolDeclarations
{
    private const int MaxName = 64;

    private readonly Dictionary<string, string> _strata;

    private ToolDeclarations(IReadOnlyList<AITool> tools, Dictionary<string, string> strata)
    {
        Tools = tools;
        _strata = strata;
    }

    /// <summary>The declarations, under the names the model sees, in the registry's order.</summary>
    public IReadOnlyList<AITool> Tools { get; }

    /// <summary>The declarations of every tool in <paramref name="registry"/>, in its order.</summary>
    public static ToolDeclarations For(ToolRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        var taken = new HashSet<string>(StringComparer.Ordinal);
        var tools = new List<AITool>(registry.Tools.Count);
        var strata = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var tool in registry.Tools)
        {
            var name = Unique(ProviderName(tool.Name), taken);
            tools.Add(AIFunctionFactory.CreateDeclaration(name, tool.Description, tool.Parameters));
            strata[name] = tool.Name;
        }

        return new ToolDeclarations(tools, strata);
    }

    /// <summary>
    /// The Strata name of the tool the model calls <paramref name="name"/>, or <paramref name="name"/> itself when no
    /// declaration has it: the registry then refuses it as not found.
    /// </summary>
    public string StrataName(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return _strata.TryGetValue(name, out var strata) ? strata : name;
    }

    /// <summary>The name the model sees for the Strata tool <paramref name="strataName"/>.</summary>
    public string? ProviderNameOf(string strataName) =>
        _strata.FirstOrDefault(pair => pair.Value == strataName).Key;

    /// <summary><paramref name="name"/> with every character outside <c>[a-zA-Z0-9_-]</c> as <c>_</c>, cut to 64.</summary>
    internal static string ProviderName(string name)
    {
        var safe = new StringBuilder(Math.Min(name.Length, MaxName));
        foreach (var rune in name.EnumerateRunes())
        {
            if (safe.Length == MaxName)
            {
                break;
            }

            safe.Append(rune.IsAscii && (char.IsAsciiLetterOrDigit((char)rune.Value) || rune.Value is '_' or '-') ? (char)rune.Value : '_');
        }

        return safe.Length == 0 ? "_" : safe.ToString();
    }

    /// <summary><paramref name="name"/>, or the first of <c>name_2</c>, <c>name_3</c>, … (cut to fit 64) not taken.</summary>
    private static string Unique(string name, HashSet<string> taken)
    {
        if (taken.Add(name))
        {
            return name;
        }

        for (var n = 2; ; n++)
        {
            var suffix = $"_{n}";
            var candidate = name[..Math.Min(name.Length, MaxName - suffix.Length)] + suffix;
            if (taken.Add(candidate))
            {
                return candidate;
            }
        }
    }
}
