using System.Threading.Channels;

namespace Datalium.Strata.Observability;

/// <summary>
/// One run's events on their way to its <see cref="IRunObserver"/>
/// (<c>design/18-Run-Observability.md</c>). Recording an event is a lock, an increment and an
/// enqueue; a pump delivers them one at a time, in sequence order, off the executor. The queue is
/// unbounded, so a slow observer only grows it and never makes the run wait.
/// </summary>
internal sealed class RunEventQueue
{
    private readonly Channel<RunEvent> _channel =
        Channel.CreateUnbounded<RunEvent>(new UnboundedChannelOptions { SingleReader = true });

    private readonly Lock _gate = new();
    private readonly string _threadId;
    private readonly string _runId;
    private long _sequence;

    public RunEventQueue(IRunObserver observer, string threadId, string runId)
    {
        _threadId = threadId;
        _runId = runId;

        // Observer code must not run inside the run's Activity, or see its AsyncLocals.
        using (ExecutionContext.SuppressFlow())
        {
            _ = Task.Run(() => PumpAsync(observer));
        }
    }

    public void Record(RunEvent runEvent, string? nodePath, int? attempt)
    {
        runEvent.ThreadId = _threadId;
        runEvent.RunId = _runId;
        runEvent.NodePath = nodePath;
        runEvent.Attempt = attempt;

        // The sequence is taken under the same lock that enqueues, so delivery order is sequence
        // order even when concurrent branches record at once.
        lock (_gate)
        {
            runEvent.Sequence = ++_sequence;
            runEvent.Timestamp = DateTimeOffset.UtcNow;
            _channel.Writer.TryWrite(runEvent);
        }
    }

    /// <summary>No more events: the pump stops once it has delivered the last one.</summary>
    public void Complete() => _channel.Writer.TryComplete();

    private async Task PumpAsync(IRunObserver observer)
    {
        await foreach (var runEvent in _channel.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                observer.OnEvent(runEvent);
            }
            catch (Exception)
            {
                // An observer can't fail the run, or stop the events after this one; it can only be counted.
                StrataTelemetry.ObserverErrors.Add(1);
            }
        }
    }
}
