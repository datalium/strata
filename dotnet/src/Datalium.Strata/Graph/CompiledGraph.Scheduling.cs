using Datalium.Strata.Interruption;
using Datalium.Strata.Scheduling;

namespace Datalium.Strata.Graph;

/// <summary>Waiting for a slot of the run's scheduler (<c>design/40-Run-Scheduler.md</c>).</summary>
public sealed partial class CompiledGraph<TState>
{
    /// <summary>
    /// Waits for a slot of <paramref name="options"/>' scheduler, before the run loads or writes anything:
    /// <see langword="null"/> when it has none. The tenant is the run's metadata's, laid over
    /// <paramref name="listed"/> (the metadata a driver listed the thread with). <paramref name="stop"/>, a driver's,
    /// ends the wait like the caller's token.
    /// </summary>
    private static ValueTask<Admission?> AdmitAsync(
        RunOptions options,
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, string>? listed = null,
        CancellationToken stop = default)
    {
        return options.Scheduler is { } scheduler
            ? WaitAsync(scheduler)
            : ValueTask.FromResult<Admission?>(null);

        async ValueTask<Admission?> WaitAsync(RunScheduler scheduler)
        {
            var key = scheduler.Options.TenantKey;
            var tenant = options.Metadata.TryGetValue(key, out var own) ? own
                : listed is not null && listed.TryGetValue(key, out var stored) ? stored
                : null;
            var request = new AdmissionRequest
            {
                Priority = options.Priority,
                Tenant = tenant,
                Deadline = options.Deadline,
                TimeProvider = options.TimeProvider,
            };
            if (!stop.CanBeCanceled)
            {
                return await scheduler.AdmitAsync(request, cancellationToken).ConfigureAwait(false);
            }

            using var either = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stop);
            return await scheduler.AdmitAsync(request, either.Token).ConfigureAwait(false);
        }
    }

    /// <summary>What a run its scheduler refused returns.</summary>
    private static GraphExecutionResult<TState>.NotAdmitted NotAdmitted(Admission refused) =>
        new GraphExecutionResult<TState>.NotAdmitted(refused.Refusal!.Value, refused.Waited);
}
