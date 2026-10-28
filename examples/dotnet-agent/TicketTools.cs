using Datalium.Strata.Tools;

namespace Datalium.Strata.Example.DotnetAgent;

/// <summary>
/// STRATA-SPEC-003 tool-calling (Phase 5): <c>Datalium.Strata.SourceGenerators</c> generates a
/// second partial declaration of this class implementing <see cref="IStrataToolProvider"/>.
/// </summary>
public partial class TicketTools
{
    [StrataTool(Name = "classify_ticket", Description = "Classifies a support ticket subject line into a category.")]
    public static async Task<string> ClassifyTicketAsync(string subject)
    {
        var lowered = subject.ToLowerInvariant();
        var category = lowered.Contains("invoice") || lowered.Contains("bill")
            ? "billing"
            : lowered.Contains("crash") || lowered.Contains("error")
                ? "technical"
                : "general";
        return await Task.FromResult(category);
    }
}
