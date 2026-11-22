// Samples for https://stratamesh.dev/tutorials/support-ticket-agent/: the tutorial's TicketTools.cs, a file of
// its own on the page too. SupportTicketAgent.cs runs it.

// docs:begin tutorials/support-ticket-agent/ticket-tools
using Datalium.Strata.Tools;

// docs:hide
namespace Datalium.Strata.DocsSnippets.Tutorials.SupportTicketAgent;

// docs:show
public partial class TicketTools
{
    [StrataTool(Name = "classify_ticket", Description = "Classifies a support ticket subject line into a category.")]
    public static string ClassifyTicket(string subject)
    {
        var lowered = subject.ToLowerInvariant();
        return lowered.Contains("invoice") || lowered.Contains("bill") ? "billing"
            : lowered.Contains("crash") || lowered.Contains("error") ? "technical"
            : "general";
    }
}
// docs:end tutorials/support-ticket-agent/ticket-tools

public class TicketToolsSamples
{
    [Theory]
    [InlineData("My invoice total looks wrong this month", "billing")]
    [InlineData("The app crashes on start", "technical")]
    [InlineData("How do I change my email?", "general")]
    public async Task ClassifiesBySubject(string subject, string category)
    {
        var tools = new ToolRegistry().Register(new TicketTools());
        var json = await tools.DispatchToolAsync("classify_ticket", $$"""{"subject":"{{subject}}"}""");
        Assert.Equal($$"""{"result":"{{category}}"}""", json);
    }
}
