using Datalium.Strata.Hosting;
using Datalium.Strata.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;

namespace Datalium.Strata.Example.DotnetAgent.Tests;

/// <summary>
/// Proves the example agent actually runs end to end in the host it is written for (the stores, the graph and the
/// runner from <see cref="SupportTicketDemo.AddSupportTickets"/>, the host's start-up checks run), not just that it
/// compiles.
/// </summary>
public class SupportTicketDemoTests
{
    [Fact]
    public async Task The_Demo_Graph_Pauses_Classifies_And_Resolves_The_Ticket()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddSupportTickets();
        using var host = builder.Build();
        await host.StartAsync();

        var (finalState, history) = await SupportTicketDemo.RunAsync(
            host.Services.GetRequiredService<StrataRunner<TicketState>>(),
            host.Services.GetRequiredService<IHistoryStore>(),
            NullLogger.Instance);

        Assert.Equal("billing", finalState.Category);
        Assert.Equal("demo-reviewer", finalState.ResolvedBy);
        Assert.Contains("billing", finalState.Resolution);

        var message = Assert.Single(history);
        Assert.Equal(MessageRole.Assistant, message.Role);
        Assert.Contains("billing", message.Content.ToString());

        await host.StopAsync();
    }
}
