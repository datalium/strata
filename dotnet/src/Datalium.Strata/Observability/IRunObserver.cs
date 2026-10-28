using System.Threading.Channels;

namespace Datalium.Strata.Observability;

/// <summary>
/// Receives a run's events (<c>design/18-Run-Observability.md</c>), set with
/// <c>RunOptions.Observer</c>. Mirrors the Rust engine's <c>RunObserver</c>.
/// </summary>
/// <remarks>
/// <see cref="OnEvent"/> is called once per event, in sequence order, never concurrently for one run,
/// and never on the executor: a slow observer only grows its own queue, and an exception from it is
/// caught and counted (<c>strata.observer.errors</c>), so neither can stall or fail the run. An
/// observer shared between runs must be thread-safe. The run doesn't wait for delivery:
/// <see cref="RunCompleted"/> may arrive after <c>RunAsync</c> has returned.
/// </remarks>
public interface IRunObserver
{
    void OnEvent(RunEvent runEvent);
}

/// <summary>
/// An observer for one run whose <see cref="Reader"/> yields its events and completes after
/// <see cref="RunCompleted"/>, so <c>await foreach (var e in channel.Reader.ReadAllAsync())</c> ends
/// with the run. Events from a second run are refused (and counted as an observer error).
/// </summary>
public sealed class RunEventChannel : IRunObserver
{
    private readonly Channel<RunEvent> _channel =
        Channel.CreateUnbounded<RunEvent>(new UnboundedChannelOptions { SingleWriter = true });

    private string? _runId;

    public ChannelReader<RunEvent> Reader => _channel.Reader;

    public void OnEvent(RunEvent runEvent)
    {
        ArgumentNullException.ThrowIfNull(runEvent);
        _runId ??= runEvent.RunId;
        if (_runId != runEvent.RunId)
        {
            throw new InvalidOperationException(
                $"a RunEventChannel serves one run: it is observing '{_runId}', not '{runEvent.RunId}'");
        }

        _channel.Writer.TryWrite(runEvent);
        if (runEvent is RunCompleted)
        {
            _channel.Writer.TryComplete();
        }
    }
}
