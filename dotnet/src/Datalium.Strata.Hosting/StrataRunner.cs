using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Observability;
using Datalium.Strata.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Datalium.Strata.Hosting;

/// <summary>
/// The defaults of every run the host starts, from <c>Strata:Runs</c>
/// (<c>design/35-Hosting-Integration.md</c>, "Run defaults").
/// </summary>
public sealed class StrataRunOptions
{
    internal const string Section = "Runs";

    /// <summary>The lease every run holds its thread under (<c>design/32-Thread-Leases.md</c>), when it has a ttl.</summary>
    public StrataLeaseOptions Lease { get; } = new();
}

/// <summary>A run's lease, as configuration can say it.</summary>
public sealed class StrataLeaseOptions
{
    /// <summary>Who holds the threads this process runs. The machine name and the process id by default.</summary>
    public string Owner { get; set; } = $"{Environment.MachineName}/{Environment.ProcessId}";

    /// <summary>
    /// How long a lease lasts unless renewed; a run renews it every third of this. Not set (the default):
    /// runs take no lease. Every worker on a thread uses leases, or none does.
    /// </summary>
    public TimeSpan? Ttl { get; set; }
}

/// <summary>
/// A compiled graph, the container's checkpoint store and the host's run defaults in one injectable object
/// (<c>design/35-Hosting-Integration.md</c>, "Run defaults"): what an application's own worker needs from
/// Strata to start, answer, recover or end a thread. Registered by
/// <see cref="StrataGraphExtensions.AddGraph{TState}"/>. The drivers run with the same defaults.
/// </summary>
public sealed class StrataRunner<TState>
{
    private readonly IServiceProvider _services;
    private readonly TimeProvider _clock;
    private readonly StrataRunOptions _defaults;

    public StrataRunner(
        CompiledGraph<TState> graph,
        ICheckpointStore store,
        IServiceProvider services,
        TimeProvider clock,
        IOptions<StrataRunOptions> defaults)
    {
        ArgumentNullException.ThrowIfNull(defaults);
        Graph = graph ?? throw new ArgumentNullException(nameof(graph));
        Store = store ?? throw new ArgumentNullException(nameof(store));
        _services = services ?? throw new ArgumentNullException(nameof(services));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _defaults = defaults.Value;
    }

    /// <summary>The compiled graph.</summary>
    public CompiledGraph<TState> Graph { get; }

    /// <summary>The container's checkpoint store.</summary>
    public ICheckpointStore Store { get; }

    /// <summary>
    /// The host's run options: <see cref="RunOptions.Services"/> is the container, the clock, the history
    /// store, the observer and the run scheduler (<c>AddScheduler</c>) are the registered ones if any, and the
    /// lease is <c>Strata:Runs:Lease</c> when it has a ttl. The arguments are this run's own;
    /// <paramref name="observer"/> replaces the registered one, and <paramref name="priority"/> is the run's
    /// priority for the scheduler.
    /// </summary>
    public RunOptions CreateOptions(
        IReadOnlyDictionary<string, string>? metadata = null,
        string? runId = null,
        DateTimeOffset? deadline = null,
        IRunObserver? observer = null,
        int priority = 0) => new()
        {
            Services = _services,
            TimeProvider = _clock,
            History = _services.GetService<IHistoryStore>(),
            Observer = observer ?? _services.GetService<IRunObserver>(),
            Lease = _defaults.Lease.Ttl is { } ttl ? new LeaseOptions(_defaults.Lease.Owner, ttl) : null,
            Metadata = metadata is null ? [] : new Dictionary<string, string>(metadata),
            RunId = runId,
            Deadline = deadline,
            Scheduler = _services.GetService<RunScheduler>(),
            Priority = priority,
        };

    /// <summary>Runs a thread from <paramref name="initial"/>. With no <paramref name="options"/>, the host's (<see cref="CreateOptions"/>).</summary>
    public Task<GraphExecutionResult<TState>> RunAsync(
        string threadId, TState initial, RunOptions? options = null, CancellationToken cancellationToken = default) =>
        Graph.RunAsync(threadId, initial, Store, options ?? CreateOptions(), cancellationToken);

    /// <summary>Answers a thread waiting for a human.</summary>
    public Task<GraphExecutionResult<TState>> ResumeAsync(
        string threadId, HumanSignal signal, RunOptions? options = null, CancellationToken cancellationToken = default) =>
        Graph.ResumeAsync(threadId, signal, Store, options ?? CreateOptions(), cancellationToken);

    /// <summary>Delivers an event to a thread waiting for it.</summary>
    public Task<GraphExecutionResult<TState>> SignalAsync(
        string threadId, EventSignal signal, RunOptions? options = null, CancellationToken cancellationToken = default) =>
        Graph.SignalAsync(threadId, signal, Store, options ?? CreateOptions(), cancellationToken);

    /// <summary>Continues a <c>running</c> thread whose run died (<c>design/33-Durable-Execution.md</c>).</summary>
    public Task<GraphExecutionResult<TState>> RecoverAsync(
        string threadId, RunOptions? options = null, CancellationToken cancellationToken = default) =>
        Graph.RecoverAsync(threadId, Store, options ?? CreateOptions(), cancellationToken);

    /// <summary>Ends a thread where it stands (<c>design/28-Ending-A-Thread.md</c>).</summary>
    public Task<GraphExecutionResult<TState>> EndAsync(string threadId, ThreadEnd? end = null, RunOptions? options = null) =>
        Graph.EndAsync(threadId, end ?? new ThreadEnd(), Store, options ?? CreateOptions());
}

/// <summary>Registers compiled graphs (<c>design/35-Hosting-Integration.md</c>, "Graphs and runs").</summary>
public static class StrataGraphExtensions
{
    /// <summary>
    /// Registers the graph <paramref name="factory"/> builds as a singleton, and a
    /// <see cref="StrataRunner{TState}"/> for it. The graph is built when the host starts, so one with a
    /// structural error stops the host from starting. One graph per state type: registering another for the
    /// same <typeparamref name="TState"/> replaces the first.
    /// </summary>
    public static IStrataBuilder AddGraph<TState>(this IStrataBuilder builder, Func<IServiceProvider, CompiledGraph<TState>> factory)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        var services = builder.Services;
        services.RemoveAll<CompiledGraph<TState>>();
        services.AddSingleton(factory);
        services.TryAddSingleton<StrataRunner<TState>>();
        StrataStartupStep.Set(services, $"graph:{typeof(TState).FullName}", (sp, _) =>
        {
            sp.GetRequiredService<CompiledGraph<TState>>();
            return Task.CompletedTask;
        });
        return builder;
    }
}
