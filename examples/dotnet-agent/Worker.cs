using Datalium.Strata.Hosting;
using Datalium.Strata.Memory;

namespace Datalium.Strata.Example.DotnetAgent;

/// <summary>
/// Runs the support-ticket demo once through the host's <see cref="StrataRunner{TState}"/>, then stops the host — a
/// real worker service would instead loop over an incoming queue, but a single deterministic run is what makes this
/// example runnable end to end in CI rather than a long-lived background process.
/// </summary>
public class Worker(
    StrataRunner<TicketState> runner,
    IHistoryStore history,
    ILogger<Worker> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await SupportTicketDemo.RunAsync(runner, history, logger, stoppingToken);
        }
        finally
        {
            lifetime.StopApplication();
        }
    }
}
