using Datalium.Strata.Cloud;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Datalium.Strata.Hosting;

/// <summary>The wake driver's settings, from <c>Strata:Wake</c>.</summary>
public sealed class WakeDriverOptions
{
    internal const string Section = "Wake";

    /// <summary>The wait between two rounds. A timer or a timeout fires at most this long after it is due.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The most threads one listing looks at: 1 to <see cref="ThreadQuery.MaxLimit"/>.</summary>
    public int BatchSize { get; set; } = ThreadQuery.DefaultLimit;

    /// <summary>Only threads whose latest checkpoint's metadata has every pair.</summary>
    public Dictionary<string, string> Metadata { get; } = new(StringComparer.Ordinal);

    /// <summary>Whether a graph with an identity wakes only the threads it wrote (<c>design/31-Graph-Identity.md</c>).</summary>
    public bool MatchGraph { get; set; } = true;

    /// <summary>The priority of the runs it wakes, for the run scheduler (<c>design/40-Run-Scheduler.md</c>).</summary>
    public int Priority { get; set; }
}

/// <summary>The recovery driver's settings, from <c>Strata:Recovery</c>.</summary>
public sealed class RecoveryDriverOptions
{
    internal const string Section = "Recovery";

    /// <summary>The wait between two rounds. A dead run is recovered at most this long after its lease runs out.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The most threads recovered before the driver lists again: 1 to <see cref="ThreadQuery.MaxLimit"/>.</summary>
    public int BatchSize { get; set; } = ThreadQuery.DefaultLimit;

    /// <summary>
    /// How old a <c>running</c> checkpoint must be before its thread is recovered
    /// (<see cref="RecoverOptions.StaleAfter"/>). Required when runs take no lease.
    /// </summary>
    public TimeSpan? StaleAfter { get; set; }

    /// <summary>The most times a thread is recovered without a node committing in between (<see cref="RecoverOptions.MaxRecoveries"/>).</summary>
    public int? MaxRecoveries { get; set; }

    /// <summary>Only threads whose latest checkpoint's metadata has every pair.</summary>
    public Dictionary<string, string> Metadata { get; } = new(StringComparer.Ordinal);

    /// <summary>Whether a graph with an identity recovers only the threads it wrote.</summary>
    public bool MatchGraph { get; set; } = true;

    /// <summary>The priority of the runs it recovers, for the run scheduler (<c>design/40-Run-Scheduler.md</c>).</summary>
    public int Priority { get; set; }
}

/// <summary>The retention driver's settings, from <c>Strata:Retention</c>.</summary>
public sealed class RetentionOptions
{
    internal const string Section = "Retention";

    /// <summary>The wait between two rounds.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>The most threads one purge deletes: 1 to <see cref="ThreadQuery.MaxLimit"/>. A round purges until one comes back short.</summary>
    public int BatchSize { get; set; } = ThreadQuery.MaxLimit;

    /// <summary>
    /// How long a thread is kept after its latest checkpoint. No default: an application says how long it
    /// keeps its threads, and the host doesn't start without it.
    /// </summary>
    public TimeSpan? OlderThan { get; set; }

    /// <summary>The thread states purged. Empty (the default): <c>completed</c> and <c>ended</c>.</summary>
    public List<CheckpointThreadState> States { get; } = [];
}

/// <summary>
/// Registers the loops an application would otherwise write itself (<c>design/35-Hosting-Integration.md</c>,
/// "Drivers"): each is a hosted service that does a round, waits, and goes again, backs off while it fails,
/// and on stop lets the run in flight finish. Every replica may run every driver.
/// </summary>
public static class StrataDriverExtensions
{
    /// <summary>
    /// Wakes <typeparamref name="TState"/>'s due threads: timers whose time has come and waits whose timeout
    /// has passed (<c>design/29-Event-Waits-And-Timers.md</c>). Needs <c>AddGraph&lt;TState&gt;</c> and a
    /// checkpoint store that lists threads.
    /// </summary>
    public static IStrataBuilder AddWakeDriver<TState>(this IStrataBuilder builder, Action<WakeDriverOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, WakeDriverOptions.Section, configure, GraphName<TState>());
        builder.Services.AddHostedService<WakeDriver<TState>>();
        StrataStartupStep.Set(builder.Services, $"driver:wake:{GraphName<TState>()}", (sp, _) =>
        {
            WakeDriver<TState>.Check(sp);
            return Task.CompletedTask;
        });
        return builder;
    }

    /// <summary>
    /// Recovers <typeparamref name="TState"/>'s <c>running</c> threads whose run died
    /// (<c>design/33-Durable-Execution.md</c>). Needs <c>AddGraph&lt;TState&gt;</c> with a graph compiled with
    /// <see cref="CheckpointPolicy.EveryNode"/>, a checkpoint store that lists threads, and either a lease in
    /// the run defaults or a <see cref="RecoveryDriverOptions.StaleAfter"/>.
    /// </summary>
    public static IStrataBuilder AddRecoveryDriver<TState>(this IStrataBuilder builder, Action<RecoveryDriverOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, RecoveryDriverOptions.Section, configure, GraphName<TState>());
        builder.Services.AddHostedService<RecoveryDriver<TState>>();
        StrataStartupStep.Set(builder.Services, $"driver:recovery:{GraphName<TState>()}", (sp, _) =>
        {
            RecoveryDriver<TState>.Check(sp);
            return Task.CompletedTask;
        });
        return builder;
    }

    /// <summary>
    /// Purges threads that are done with (<c>design/34-Storage-Hardening.md</c>, "Retention"). Needs a
    /// checkpoint store with the retention port and a <see cref="RetentionOptions.OlderThan"/>.
    /// </summary>
    public static IStrataBuilder AddRetention(this IStrataBuilder builder, Action<RetentionOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        StrataConfiguration.Bind(builder.Services, RetentionOptions.Section, configure);
        builder.Services.AddHostedService<RetentionDriver>();
        StrataStartupStep.Set(builder.Services, "driver:retention", (sp, _) =>
        {
            RetentionDriver.Check(sp);
            return Task.CompletedTask;
        });
        return builder;
    }

    /// <summary>
    /// Runs the Strata Cloud decision worker for <typeparamref name="TState"/>
    /// (<c>design/30-Strata-Cloud-Store.md</c>): it resumes the threads reviewers decide in the console. Needs
    /// <c>AddGraph&lt;TState&gt;</c> and <c>AddCloudCheckpoints</c>. A worker that stops on an error is
    /// started again after a wait. <paramref name="options"/>'s runs use the host's run defaults unless it
    /// gives its own.
    /// </summary>
    public static IStrataBuilder AddDecisionWorker<TState>(
        this IStrataBuilder builder, Func<IServiceProvider, DecisionWorkerOptions<TState>>? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType == typeof(DecisionDriverSettings<TState>))
            {
                services.RemoveAt(i);
            }
        }

        services.AddSingleton(new DecisionDriverSettings<TState>(options));
        services.AddHostedService<DecisionDriver<TState>>();
        StrataStartupStep.Set(services, $"driver:decisions:{GraphName<TState>()}", (sp, _) =>
        {
            DecisionDriver<TState>.Check(sp);
            return Task.CompletedTask;
        });
        return builder;
    }

    internal static string GraphName<TState>() => typeof(TState).FullName ?? typeof(TState).Name;

    internal static CompiledGraph<TState> Graph<TState>(IServiceProvider services, string driver) =>
        services.GetService<CompiledGraph<TState>>()
        ?? throw new InvalidOperationException(
            $"the {driver} driver for {typeof(TState).Name} has no graph: register one with AddGraph<{typeof(TState).Name}>");

    internal static QueryableStore Store(IServiceProvider services, string driver) =>
        new(
            services.GetService<ICheckpointStore>()
                ?? throw new InvalidOperationException($"the {driver} driver has no checkpoint store: register one with AddStrata().Add…Checkpoints()"),
            services.GetService<ICheckpointQuery>()
                ?? throw new InvalidOperationException(
                    $"the {driver} driver needs a checkpoint store that lists threads (ICheckpointQuery), and the registered one doesn't"));

    internal static void CheckPacing(string driver, TimeSpan interval, int batchSize)
    {
        if (interval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException($"the {driver} driver's Interval must be more than zero");
        }

        if (batchSize is < 1 or > ThreadQuery.MaxLimit)
        {
            throw new InvalidOperationException($"the {driver} driver's BatchSize must be between 1 and {ThreadQuery.MaxLimit}");
        }
    }
}

internal sealed class WakeDriver<TState>(
    IServiceProvider services,
    IOptionsMonitor<WakeDriverOptions> options,
    StrataDriverStatus status,
    TimeProvider clock,
    ILoggerFactory loggers)
    : StrataDriver("wake", $"wake:{typeof(TState).Name}", status, clock, loggers)
{
    private readonly WakeDriverOptions _options = options.Get(StrataDriverExtensions.GraphName<TState>());

    protected override TimeSpan Interval => _options.Interval;

    public static void Check(IServiceProvider services)
    {
        var options = services.GetRequiredService<IOptionsMonitor<WakeDriverOptions>>().Get(StrataDriverExtensions.GraphName<TState>());
        StrataDriverExtensions.CheckPacing("wake", options.Interval, options.BatchSize);
        StrataDriverExtensions.Graph<TState>(services, "wake");
        StrataDriverExtensions.Store(services, "wake");
    }

    protected override async Task<int> RoundAsync(CancellationToken stop)
    {
        var runner = services.GetRequiredService<StrataRunner<TState>>();
        var store = StrataDriverExtensions.Store(services, "wake");
        var woken = 0;
        while (true)
        {
            // No token of the call's own: the run in flight finishes when the host stops.
            var report = await runner.Graph.WakeDueAsync(
                    store,
                    new WakeOptions
                    {
                        Limit = _options.BatchSize,
                        Metadata = _options.Metadata,
                        MatchGraph = _options.MatchGraph,
                        Run = runner.CreateOptions(priority: _options.Priority),
                        Stop = stop,
                    },
                    CancellationToken.None)
                .ConfigureAwait(false);
            woken += report.Woken.Count;

            // More may be due behind a full batch. A batch that woke nothing would be listed again as it is.
            var full = report.Woken.Count + report.Skipped.Count >= _options.BatchSize;
            if (!full || report.Woken.Count == 0 || stop.IsCancellationRequested)
            {
                return woken;
            }
        }
    }
}

internal sealed class RecoveryDriver<TState>(
    IServiceProvider services,
    IOptionsMonitor<RecoveryDriverOptions> options,
    StrataDriverStatus status,
    TimeProvider clock,
    ILoggerFactory loggers)
    : StrataDriver("recovery", $"recovery:{typeof(TState).Name}", status, clock, loggers)
{
    private readonly RecoveryDriverOptions _options = options.Get(StrataDriverExtensions.GraphName<TState>());

    protected override TimeSpan Interval => _options.Interval;

    public static void Check(IServiceProvider services)
    {
        var options = services.GetRequiredService<IOptionsMonitor<RecoveryDriverOptions>>().Get(StrataDriverExtensions.GraphName<TState>());
        StrataDriverExtensions.CheckPacing("recovery", options.Interval, options.BatchSize);
        var graph = StrataDriverExtensions.Graph<TState>(services, "recovery");
        StrataDriverExtensions.Store(services, "recovery");
        if (graph.Checkpointing != CheckpointPolicy.EveryNode)
        {
            throw new InvalidOperationException(
                $"the recovery driver for {typeof(TState).Name} needs a graph compiled with CheckpointPolicy.EveryNode: only such a graph leaves a running thread to recover");
        }

        if (options.StaleAfter is null && services.GetRequiredService<IOptions<StrataRunOptions>>().Value.Lease.Ttl is null)
        {
            throw new InvalidOperationException(
                "the recovery driver needs Strata:Runs:Lease:Ttl or Strata:Recovery:StaleAfter: without leases only the age of a running checkpoint says its run died");
        }
    }

    protected override async Task<int> RoundAsync(CancellationToken stop)
    {
        var runner = services.GetRequiredService<StrataRunner<TState>>();
        var store = StrataDriverExtensions.Store(services, "recovery");
        var recovered = 0;
        while (true)
        {
            var report = await runner.Graph.RecoverStaleAsync(
                    store,
                    new RecoverOptions
                    {
                        StaleAfter = _options.StaleAfter,
                        MaxRecoveries = _options.MaxRecoveries,
                        Metadata = _options.Metadata,
                        MatchGraph = _options.MatchGraph,
                        Limit = _options.BatchSize,
                        Run = runner.CreateOptions(priority: _options.Priority),
                        Stop = stop,
                    },
                    CancellationToken.None)
                .ConfigureAwait(false);
            recovered += report.Recovered.Count;
            if (report.Recovered.Count < _options.BatchSize || stop.IsCancellationRequested)
            {
                return recovered;
            }
        }
    }
}

internal sealed class RetentionDriver(
    IServiceProvider services,
    IOptions<RetentionOptions> options,
    StrataDriverStatus status,
    TimeProvider clock,
    ILoggerFactory loggers)
    : StrataDriver("retention", "retention", status, clock, loggers)
{
    private readonly RetentionOptions _options = options.Value;
    private readonly TimeProvider _clock = clock;

    protected override TimeSpan Interval => _options.Interval;

    public static void Check(IServiceProvider services)
    {
        var options = services.GetRequiredService<IOptions<RetentionOptions>>().Value;
        StrataDriverExtensions.CheckPacing("retention", options.Interval, options.BatchSize);
        if (options.OlderThan is not { } olderThan || olderThan <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                "the retention driver needs Strata:Retention:OlderThan: how long a thread is kept after its latest checkpoint");
        }

        if (services.GetService<ICheckpointRetention>() is null)
        {
            throw new InvalidOperationException(
                "the retention driver needs a checkpoint store that purges (ICheckpointRetention), and the registered one doesn't");
        }
    }

    protected override async Task<int> RoundAsync(CancellationToken stop)
    {
        var retention = services.GetRequiredService<ICheckpointRetention>();
        IReadOnlySet<CheckpointThreadState>? states =
            _options.States.Count == 0 ? null : new HashSet<CheckpointThreadState>(_options.States);
        var purged = 0;
        while (true)
        {
            var batch = await retention
                .PurgeAsync(_clock.GetUtcNow() - _options.OlderThan!.Value, states, _options.BatchSize, CancellationToken.None)
                .ConfigureAwait(false);
            purged += batch;
            if (batch < _options.BatchSize || stop.IsCancellationRequested)
            {
                return purged;
            }
        }
    }
}

/// <summary>How <c>AddDecisionWorker</c> was asked to configure the worker for <typeparamref name="TState"/>.</summary>
internal sealed record DecisionDriverSettings<TState>(Func<IServiceProvider, DecisionWorkerOptions<TState>>? Options);

internal sealed class DecisionDriver<TState>(
    IServiceProvider services,
    DecisionDriverSettings<TState> settings,
    StrataDriverStatus status,
    TimeProvider clock,
    ILoggerFactory loggers)
    : StrataDriver("decisions", $"decisions:{typeof(TState).Name}", status, clock, loggers)
{
    /// <summary>The wait before a worker that stopped on an error is started again; doubled while it keeps stopping.</summary>
    protected override TimeSpan Interval => TimeSpan.FromSeconds(1);

    public static void Check(IServiceProvider services)
    {
        StrataDriverExtensions.Graph<TState>(services, "decisions");
        if (services.GetService<StrataCloudDecisions>() is null)
        {
            throw new InvalidOperationException(
                "the decision worker needs Strata Cloud's decisions client: register it with AddCloudCheckpoints()");
        }
    }

    protected override async Task<int> RoundAsync(CancellationToken stop)
    {
        var runner = services.GetRequiredService<StrataRunner<TState>>();
        var options = settings.Options?.Invoke(services) ?? new DecisionWorkerOptions<TState>();
        if (options.RunOptions is null)
        {
            options = options with { RunOptions = _ => runner.CreateOptions() };
        }

        // The worker's own loop: it returns when the host stops (never cancelling a resume), and throws on an
        // error it doesn't retry, which the driver's loop counts and waits out.
        var worker = new DecisionWorker<TState>(
            runner.Graph, runner.Store, services.GetRequiredService<StrataCloudDecisions>(), options);
        await worker.RunAsync(stop).ConfigureAwait(false);
        return 0;
    }
}
