using Datalium.Strata.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Datalium.Strata.Hosting;

/// <summary>
/// The run scheduler's settings, from <c>Strata:Scheduler</c> (<c>design/40-Run-Scheduler.md</c>, "Hosting"):
/// <see cref="RunSchedulerOptions"/> as configuration can say it.
/// </summary>
public sealed class StrataSchedulerOptions
{
    internal const string Section = "Scheduler";

    /// <inheritdoc cref="RunSchedulerOptions.Name"/>
    public string Name { get; set; } = "default";

    /// <inheritdoc cref="RunSchedulerOptions.MaxConcurrent"/>
    public int MaxConcurrent { get; set; } = 16;

    /// <inheritdoc cref="RunSchedulerOptions.MaxQueued"/>
    public int MaxQueued { get; set; } = 1024;

    /// <inheritdoc cref="RunSchedulerOptions.MaxQueueTime"/>
    public TimeSpan? MaxQueueTime { get; set; }

    /// <inheritdoc cref="RunSchedulerOptions.Reservations"/>
    public List<StrataSlotReservation> Reservations { get; } = [];

    /// <inheritdoc cref="RunSchedulerOptions.TenantKey"/>
    public string TenantKey { get; set; } = "tenant";

    /// <inheritdoc cref="RunSchedulerOptions.TenantLimit"/>
    public int? TenantLimit { get; set; }

    /// <inheritdoc cref="RunSchedulerOptions.TenantLimits"/>
    public Dictionary<string, int> TenantLimits { get; } = new(StringComparer.Ordinal);

    /// <inheritdoc cref="RunSchedulerOptions.TenantMaxQueued"/>
    public int? TenantMaxQueued { get; set; }

    internal RunSchedulerOptions ToEngine() => new()
    {
        Name = Name,
        MaxConcurrent = MaxConcurrent,
        MaxQueued = MaxQueued,
        MaxQueueTime = MaxQueueTime,
        Reservations = [.. Reservations.Select(r => new SlotReservation(r.MinPriority, r.Slots))],
        TenantKey = TenantKey,
        TenantLimit = TenantLimit,
        TenantLimits = new Dictionary<string, int>(TenantLimits, StringComparer.Ordinal),
        TenantMaxQueued = TenantMaxQueued,
    };
}

/// <summary>A <see cref="SlotReservation"/> as configuration can say it.</summary>
public sealed class StrataSlotReservation
{
    /// <inheritdoc cref="SlotReservation.MinPriority"/>
    public int MinPriority { get; set; }

    /// <inheritdoc cref="SlotReservation.Slots"/>
    public int Slots { get; set; }
}

/// <summary>Registers the run scheduler (<c>design/40-Run-Scheduler.md</c>, "Hosting").</summary>
public static class StrataSchedulerExtensions
{
    /// <summary>
    /// Registers one <see cref="RunScheduler"/>, from <c>Strata:Scheduler</c> and then
    /// <paramref name="configure"/>. Every run <see cref="StrataRunner{TState}"/> starts, and every driver's, waits
    /// for its slots. It is made when the host starts, so invalid options stop the host from starting, and closed
    /// when the host begins to stop, so a run still waiting is refused instead of holding the shutdown up. Calling it
    /// again changes the options of the one scheduler.
    /// </summary>
    public static IStrataBuilder AddScheduler(this IStrataBuilder builder, Action<StrataSchedulerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        StrataConfiguration.Bind(services, StrataSchedulerOptions.Section, configure);
        services.TryAddSingleton(sp => new RunScheduler(sp.GetRequiredService<IOptions<StrataSchedulerOptions>>().Value.ToEngine()));
        if (!services.Any(d => d.ImplementationType == typeof(StrataSchedulerCloser)))
        {
            services.AddHostedService<StrataSchedulerCloser>();
        }

        StrataStartupStep.Set(services, "scheduler", (sp, _) =>
        {
            sp.GetRequiredService<RunScheduler>();
            return Task.CompletedTask;
        });
        return builder;
    }
}

/// <summary>Closes the scheduler as the host begins to stop, before any hosted service's own stop.</summary>
internal sealed class StrataSchedulerCloser(RunScheduler scheduler) : IHostedLifecycleService
{
    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        scheduler.Close();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
