using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Datalium.Strata.Hosting;

/// <summary>
/// What <see cref="StrataServiceCollectionExtensions.AddStrata"/> returns: the place Strata's stores, graphs,
/// drivers, health checks and instrumentation are added to a container
/// (<c>design/35-Hosting-Integration.md</c>).
/// </summary>
public interface IStrataBuilder
{
    /// <summary>The container being configured.</summary>
    IServiceCollection Services { get; }
}

/// <summary>Registers Strata in a container (<c>design/35-Hosting-Integration.md</c>).</summary>
public static class StrataServiceCollectionExtensions
{
    /// <summary>
    /// Starts registering Strata: the run defaults (<see cref="StrataRunOptions"/>, from
    /// <c>Strata:Runs</c>), the drivers' status, and the service that prepares stores and graphs when the host
    /// starts. Calling it again adds nothing and returns a builder over the same container.
    /// </summary>
    public static IStrataBuilder AddStrata(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddLogging();
        services.TryAddSingleton(TimeProvider.System);
        StrataConfiguration.Bind<StrataRunOptions>(services, StrataRunOptions.Section, configure: null);
        services.TryAddSingleton<StrataDriverStatus>();
        services.TryAddSingleton<IStrataDriverStatus>(sp => sp.GetRequiredService<StrataDriverStatus>());
        services.AddHostedService<StrataStartup>();
        return new StrataBuilder(services);
    }

    /// <summary><see cref="AddStrata(IServiceCollection)"/>, then <paramref name="configure"/> for the run defaults.</summary>
    public static IStrataBuilder AddStrata(this IServiceCollection services, Action<StrataRunOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = services.AddStrata();
        services.Configure(configure);
        return builder;
    }

    private sealed class StrataBuilder(IServiceCollection services) : IStrataBuilder
    {
        public IServiceCollection Services { get; } = services;
    }
}

/// <summary>Binding the hosting package's options from the <c>Strata</c> configuration section.</summary>
internal static class StrataConfiguration
{
    public const string Root = "Strata";

    /// <summary>
    /// Has <typeparamref name="TOptions"/> (named <paramref name="name"/>) read <c>Strata:&lt;section&gt;</c>
    /// when there is a configuration, then <paramref name="configure"/>: a delegate is applied over what was
    /// configured. The section is bound once however many registrations ask for it.
    /// </summary>
    public static void Bind<TOptions>(
        IServiceCollection services, string section, Action<TOptions>? configure, string? name = null)
        where TOptions : class
    {
        name ??= Options.DefaultName;
        var options = services.AddOptions<TOptions>(name);
        var marker = new BoundSection(typeof(TOptions), name);
        if (!services.Any(d => marker.Equals(d.ImplementationInstance)))
        {
            services.AddSingleton(marker);
            options.Configure<IServiceProvider>((bound, sp) =>
                sp.GetService<IConfiguration>()?.GetSection($"{Root}:{section}").Bind(bound));
        }

        if (configure is not null)
        {
            options.Configure(configure);
        }
    }

    private sealed record BoundSection(Type Options, string Name);
}

/// <summary>
/// Something done once when the host starts, before the drivers do: migrating a store, compiling a graph,
/// checking that a driver has what it needs. One that throws stops the host from starting.
/// </summary>
internal sealed record StrataStartupStep(string Name, Func<IServiceProvider, CancellationToken, Task> Run)
{
    /// <summary>Adds the step, in place of any earlier one of the same name.</summary>
    public static void Set(IServiceCollection services, string name, Func<IServiceProvider, CancellationToken, Task> run)
    {
        Remove(services, name);
        services.AddSingleton(new StrataStartupStep(name, run));
    }

    public static void Remove(IServiceCollection services, string name)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ImplementationInstance is StrataStartupStep step && step.Name == name)
            {
                services.RemoveAt(i);
            }
        }
    }
}

/// <summary>
/// Runs the start-up steps in the order they were registered (<c>design/35-Hosting-Integration.md</c>,
/// "Start-up"): a host that started has stores that are current and graphs that are valid.
/// </summary>
internal sealed class StrataStartup(
    IServiceProvider services, IEnumerable<StrataStartupStep> steps, ILogger<StrataStartup> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var step in steps)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await step.Run(services, cancellationToken).ConfigureAwait(false);
            logger.LogDebug("Strata start-up step {Step} done", step.Name);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
