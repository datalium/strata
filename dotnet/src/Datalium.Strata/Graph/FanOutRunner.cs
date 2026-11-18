using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;
using Datalium.Strata.Observability;

namespace Datalium.Strata.Graph;

/// <summary>
/// Starts one branch of a fan-out (a parallel group's branch, or a map's item): its index in
/// declaration order, and its <paramref name="launch"/> inputs. Called on the executor, inside the
/// branch's span; the task it returns is the branch's work.
/// </summary>
internal delegate Task<TResult> BranchLauncher<TResult>(int index, BranchLaunch launch);

/// <summary>
/// What a branch is started with: its qualified path (<c>"&lt;group&gt;/&lt;branch&gt;"</c>), its span,
/// and its token: the siblings' one, or with a timeout one of its own that the timeout cancels too.
/// </summary>
internal readonly record struct BranchLaunch(string Path, Activity? Activity, CancellationToken Token);

/// <summary>What a fan-out calls its branches in messages: <c>branch</c> for a group, <c>item</c> for a map.</summary>
internal sealed record FanOutNoun(string One, string Many)
{
    public static readonly FanOutNoun Branch = new("branch", "branches");

    public static readonly FanOutNoun Item = new("item", "items");
}

/// <summary>
/// How a fan-out runs (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>, <c>design/27-Map-Fan-Out.md</c>):
/// a group's or a map's options, what it calls its branches, and whether a branch it gives up on winds
/// down (<paramref name="WindsDown"/>: a map item, which is a graph) or is abandoned (a group's branch,
/// which is a handler).
/// </summary>
internal sealed record FanOutSettings<TResult>(
    int? MaxConcurrency,
    TimeSpan? Timeout,
    bool CancelSiblingsOnFailure,
    PartialResults Partial,
    JoinPolicy<TResult> Join,
    FanOutNoun Noun,
    bool WindsDown);

/// <summary>
/// Runs a fan-out's branches concurrently and decides when it is done: the runner of a parallel group
/// (<see cref="ParallelGroupRunner"/>) and of a map (<see cref="MapRunner"/>). Both are registered as a
/// fallible node whose handler runs this and then the reducer, which is why retry, fallback and
/// <c>Failed</c> checkpoints need no executor changes.
/// </summary>
internal static class FanOutRunner
{
    /// <summary>How one branch settled, as its <see cref="Branch{TResult}"/> reports it to the fan-out.</summary>
    private readonly record struct Settled<TResult>(BranchStatus Status, TResult? State, NodeException? Error, Exception? Bug);

    /// <summary>
    /// Starts the branches, at most <see cref="FanOutSettings{TResult}.MaxConcurrency"/> at a time, in
    /// declaration order, and settles them until the join is decided
    /// (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>).
    ///
    /// The join is met when enough branches succeeded (or, for <c>FirstMatching</c>, matched); the
    /// branches still running are then cancelled as having lost the race, and the rest never start.
    /// A failure, or a branch timeout, matters only once the join can no longer be met: then
    /// <see cref="PartialResults.FailGroup"/> fails the fan-out, at once (cancelling the rest) unless
    /// <see cref="FanOutSettings{TResult}.CancelSiblingsOnFailure"/> is off, and
    /// <see cref="PartialResults.KeepSucceeded"/> waits for every branch and returns the outcomes,
    /// flagged unmet, failing only if nothing succeeded. A join that needs nothing (an <c>All</c> map
    /// with no items) is met before anything starts; one that needs more branches than there are can't
    /// be met, which is decided before anything starts too.
    ///
    /// The fan-out fails transiently if a retry could meet the join (each transient failure counting
    /// as a branch that might succeed), carrying the longest retry hint among its failures
    /// (<c>design/25-Retry-Backoff.md</c>); the message names each failure in declaration order. A
    /// branch that throws anything but a <see cref="NodeException"/> is a bug, rethrown as it is.
    ///
    /// The attempt's token cancels every branch too; the fan-out then throws
    /// <see cref="OperationCanceledException"/> and the executor decides whether that was the run being
    /// cancelled or the node's timeout.
    ///
    /// Each branch that starts gets a span and a <c>BranchStarted</c> / <c>BranchCompleted</c> pair
    /// (<c>design/18-Run-Observability.md</c>), ended when the fan-out sees it settle or gives up on it,
    /// with a reason when the result alone doesn't say why; a join other than <c>All</c> reports its
    /// decision as <c>JoinDecided</c>. A branch given up on is abandoned, or, when
    /// <see cref="FanOutSettings{TResult}.WindsDown"/>, waited for until it has stopped, so everything
    /// it reports comes before its <c>BranchCompleted</c> (<c>design/27-Map-Fan-Out.md</c>).
    /// </summary>
    internal static async Task<BranchOutcomes<TResult>> RunAsync<TResult>(
        IReadOnlyList<string> names,
        BranchLauncher<TResult> launch,
        FanOutSettings<TResult> settings,
        AttemptInputs inputs)
    {
        var count = names.Count;
        var join = settings.Join;
        var needed = join.Kind switch
        {
            JoinKind.All => count,
            JoinKind.Quorum => join.Count ?? count,
            _ => 1,
        };
        var limit = settings.MaxConcurrency ?? count;
        var groupActivity = Activity.Current;

        using var siblings = CancellationTokenSource.CreateLinkedTokenSource(inputs.Token);

        // Where the branches report how they settled, and the one registration that wakes the loop when
        // the attempt's token fires (design/49-Burst-Throughput.md).
        var inbox = new Inbox<TResult>(count);
        using var wake = inputs.Token.UnsafeRegister(static state => ((Inbox<TResult>)state!).Cancel(), inbox);

        // A branch's own timer and token, only when branches have a timeout.
        List<CancellationTokenSource>? sources = null;
        var outcomes = new BranchOutcome<TResult>?[count];
        var scopes = new BranchScope?[count];
        var running = new bool[count];
        var inFlight = 0;
        var deciding = new List<int>();
        var next = 0;
        var counted = 0;
        var decided = false;
        var met = false;
        var startMore = true;

        try
        {
            if (needed == 0)
            {
                decided = met = true;
            }
            else if (count < needed)
            {
                decided = true;
                Decide(met: false);
                startMore = settings.Partial == PartialResults.KeepSucceeded;
            }

            StartMore();
            while (inFlight > 0)
            {
                await inbox.WaitAsync(cancellable: true).ConfigureAwait(false);
                inputs.Token.ThrowIfCancellationRequested();
                if (!inbox.TryTake(out var index, out var settled))
                {
                    continue;
                }

                running[index] = false;
                inFlight--;
                Record(index, settled);

                if (!decided && counted >= needed)
                {
                    decided = met = true;
                    Decide(met: true);
                    await GiveUpAsync(BranchEndReason.LostRace).ConfigureAwait(false);
                    break;
                }

                if (!decided && counted + inFlight + (count - next) < needed)
                {
                    decided = true;
                    Decide(met: false);
                    if (settings.Partial == PartialResults.FailGroup)
                    {
                        startMore = false;
                        if (settings.CancelSiblingsOnFailure)
                        {
                            await GiveUpAsync(BranchEndReason.SiblingFailed).ConfigureAwait(false);
                            break;
                        }
                    }
                }

                StartMore();
            }
        }
        catch (OperationCanceledException) when (inputs.Token.IsCancellationRequested)
        {
            await GiveUpAsync(reason: null).ConfigureAwait(false);
            throw;
        }
        finally
        {
            foreach (var source in sources ?? [])
            {
                source.Dispose();
            }
        }

        // Branches that never started.
        for (var i = 0; i < count; i++)
        {
            outcomes[i] ??= new BranchOutcome<TResult> { Name = names[i], Status = BranchStatus.Cancelled };
        }

        var results = new BranchOutcomes<TResult>(
            [.. outcomes.Select(o => o!)], met, met ? [.. deciding.Select(i => names[i])] : []);
        if (met || (settings.Partial == PartialResults.KeepSucceeded && results.Succeeded.Count > 0))
        {
            return results;
        }

        throw Failure(join, needed, results, settings.Noun);

        void StartMore()
        {
            while (startMore && next < count && inFlight < limit)
            {
                var index = next++;
                running[index] = true;
                inFlight++;
                Start(index);
            }
        }

        // Starts one branch inside its own span, which the launcher (and a sync handler's thread) sees
        // as current; the fan-out's span is current again once it has started. Its token is the
        // siblings' one, plus its own timeout on the run's clock when there is one. Without a timeout
        // nothing cancels a branch apart from its siblings, so it watches the siblings' token itself
        // (design/49-Burst-Throughput.md). The branch reports to the inbox once it has settled.
        void Start(int index)
        {
            CancellationTokenSource? timer = null;
            var token = siblings.Token;
            if (settings.Timeout is { } timeout)
            {
                timer = new CancellationTokenSource(timeout, inputs.Level.Run.Clock);
                var own = CancellationTokenSource.CreateLinkedTokenSource(siblings.Token, timer.Token);
                (sources ??= []).Add(timer);
                sources.Add(own);
                token = own.Token;
            }

            var path = $"{inputs.Context.NodePath}/{names[index]}";
            var scope = inputs.Level.Run.Telemetry.BranchStarted(path, inputs.Context.Attempt);
            scopes[index] = scope;
            Task<TResult> task;
            try
            {
                task = launch(index, new BranchLaunch(path, scope.Activity, token));
            }
            catch (Exception ex)
            {
                task = Task.FromException<TResult>(ex);
            }
            finally
            {
                Activity.Current = groupActivity;
            }

            new Branch<TResult>(inbox, index, names[index], task, token, timer, siblings.Token, settings).Watch();
        }

        // Counts a settled branch towards the join and ends its scope.
        void Record(int index, Settled<TResult> settled)
        {
            if (settled.Bug is { } bug)
            {
                scopes[index]!.Completed(BranchResult.Failed, bug);
                GiveUp(reason: null);
                ExceptionDispatchInfo.Capture(bug).Throw();
            }

            outcomes[index] = new BranchOutcome<TResult>
            {
                Name = names[index],
                Status = settled.Status,
                State = settled.State,
                Error = settled.Error,
            };
            switch (settled.Status)
            {
                case BranchStatus.Succeeded:
                    scopes[index]!.Completed(BranchResult.Succeeded, error: null);
                    if (!decided && (join.Predicate is not { } predicate || predicate(settled.State!)))
                    {
                        counted++;
                        deciding.Add(index);
                    }

                    break;
                case BranchStatus.TimedOut:
                    scopes[index]!.Completed(BranchResult.Failed, settled.Error, BranchEndReason.TimedOut);
                    break;
                case BranchStatus.Failed:
                    scopes[index]!.Completed(BranchResult.Failed, settled.Error);
                    break;
                default:
                    scopes[index]!.Completed(BranchResult.Cancelled, error: null);
                    break;
            }
        }

        void Decide(bool met)
        {
            if (join.Kind != JoinKind.All)
            {
                inputs.Level.Run.Telemetry.JoinDecided(
                    inputs.Context.NodePath,
                    inputs.Context.Attempt,
                    Wire(join.Kind),
                    met,
                    met ? [.. deciding.Select(i => names[i])] : [],
                    groupActivity);
            }
        }

        // Stops waiting for the branches still running: the ones that had already settled count as
        // what they were; the rest are told to stop and, when they wind down, waited for; then they are
        // reported cancelled for `reason`.
        async Task GiveUpAsync(BranchEndReason? reason)
        {
            startMore = false;
            RecordSettled();
            siblings.Cancel();
            if (settings.WindsDown)
            {
                // Each one still running reports once it has stopped, whatever stopped it. The attempt's
                // token doesn't end this wait: what it cancelled is what is being waited for.
                for (var stopping = inFlight; stopping > 0;)
                {
                    await inbox.WaitAsync(cancellable: false).ConfigureAwait(false);
                    while (inbox.TryTake(out _, out _))
                    {
                        stopping--;
                    }
                }
            }

            ReportCancelled(reason);
        }

        // GiveUpAsync without waiting: for a bug, which propagates at once.
        void GiveUp(BranchEndReason? reason)
        {
            startMore = false;
            RecordSettled();
            siblings.Cancel();
            ReportCancelled(reason);
        }

        // The branches that had settled when the fan-out looked count as what they were, recorded in
        // declaration order whatever order they settled in; a bug among them is left unreported.
        void RecordSettled()
        {
            List<(int Index, Settled<TResult> Settled)>? settled = null;
            while (inbox.TryTake(out var index, out var outcome))
            {
                (settled ??= []).Add((index, outcome));
            }

            if (settled is null)
            {
                return;
            }

            settled.Sort(static (a, b) => a.Index.CompareTo(b.Index));
            foreach (var (index, outcome) in settled)
            {
                running[index] = false;
                inFlight--;
                if (outcome.Bug is null)
                {
                    Record(index, outcome);
                }
            }
        }

        void ReportCancelled(BranchEndReason? reason)
        {
            for (var index = 0; index < count; index++)
            {
                if (running[index])
                {
                    running[index] = false;
                    scopes[index]!.Completed(BranchResult.Cancelled, error: null, reason);
                    outcomes[index] = new BranchOutcome<TResult> { Name = names[index], Status = BranchStatus.Cancelled };
                }
            }

            inFlight = 0;
        }
    }

    /// <summary>
    /// The fan-out's failure, once its join wasn't met (<c>design/26-Parallel-Group-Controls-And-Joins.md</c>):
    /// transient if a retry could meet it, with the longest retry hint among the transient failures.
    /// </summary>
    private static NodeException Failure<TResult>(
        JoinPolicy<TResult> join, int needed, BranchOutcomes<TResult> results, FanOutNoun noun)
    {
        var failures = results.Where(r => r.Error is not null).ToList();
        var parts = failures.Select(r => $"{noun.One} '{r.Name}': {r.Error!.Message}").ToList();
        var succeeded = results.Succeeded.Count;
        if (join.Kind == JoinKind.FirstMatching)
        {
            parts.Insert(0, $"no {noun.One} matched the join ({succeeded} succeeded)");
        }
        else if (join.Kind == JoinKind.Quorum)
        {
            parts.Insert(0, $"quorum of {needed} not reached: {succeeded} of {results.Count} {noun.Many} succeeded");
        }
        else if (parts.Count == 0)
        {
            parts.Add($"no {noun.Many} to run");
        }

        var message = string.Join("; ", parts);
        var inner = failures.FirstOrDefault()?.Error;
        var fatal = failures.Count(r => r.Error!.Kind == NodeFailureKind.Fatal);
        return results.Count - fatal >= needed
            ? NodeException.Transient(
                message,
                failures.Where(r => r.Error!.Kind == NodeFailureKind.Transient).Max(r => r.Error!.RetryAfter),
                inner)
            : new NodeException(NodeFailureKind.Fatal, message, inner);
    }

    /// <summary>A join policy as events and span attributes write it.</summary>
    private static string Wire(JoinKind kind) => kind switch
    {
        JoinKind.FirstSuccess => "first_success",
        JoinKind.FirstMatching => "first_matching",
        JoinKind.Quorum => "quorum",
        _ => "all",
    };

    /// <summary>
    /// One started branch, as its fan-out watches it (<c>design/49-Burst-Throughput.md</c>): it reports how the
    /// branch settled to the fan-out's <see cref="Inbox{TResult}"/>, once, when the branch's task completes.
    /// A branch that is abandoned rather than waited for, and has a timeout, is also reported the moment its
    /// timeout fires, though its handler may run on. One that winds down (a map's item) is waited for until it
    /// has stopped, and counts as stopped by its token if it didn't succeed. Either way, its own timeout makes it
    /// <see cref="BranchStatus.TimedOut"/> (a transient failure), and anything else that fired its token
    /// <see cref="BranchStatus.Cancelled"/>. The task's result is always taken when it completes, so a late
    /// exception from a branch given up on is observed.
    /// </summary>
    private sealed class Branch<TResult>(
        Inbox<TResult> inbox,
        int index,
        string name,
        Task<TResult> task,
        CancellationToken token,
        CancellationTokenSource? timer,
        CancellationToken siblings,
        FanOutSettings<TResult> settings)
    {
        // 1 once the branch has been reported: whichever of its completion and its timeout comes first.
        private int _reported;

        // The branch's timeout, watched only when it is abandoned rather than waited for.
        private CancellationTokenRegistration _timeout;

        /// <summary>
        /// Reports the branch at once if its task has already completed, as a synchronous handler's has, so
        /// the fan-out sees it in the order it started; otherwise when it completes, or its timeout fires.
        /// </summary>
        public void Watch()
        {
            if (task.IsCompleted)
            {
                OnCompleted();
                return;
            }

            if (timer is not null && !settings.WindsDown)
            {
                _timeout = timer.Token.UnsafeRegister(static state => ((Branch<TResult>)state!).OnTimedOut(), this);
            }

            task.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(OnCompleted);
        }

        private void OnCompleted()
        {
            // Unregister, not Dispose: Dispose would wait for a timeout callback already running, which may
            // be running the fan-out's loop. The first report wins either way.
            _timeout.Unregister();
            Settled<TResult> settled;
            try
            {
                settled = new(BranchStatus.Succeeded, task.GetAwaiter().GetResult(), null, null);
            }
            catch (Exception ex) when (token.IsCancellationRequested && (settings.WindsDown || ex is OperationCanceledException))
            {
                settled = Stopped();
            }
            catch (NodeException ex)
            {
                settled = new(BranchStatus.Failed, default, ex, null);
            }
            catch (Exception ex)
            {
                // Not a NodeException, so not recovery-managed: propagate like a plain node's bug would.
                settled = new(BranchStatus.Failed, default, null, ex);
            }

            Report(settled);
        }

        private void OnTimedOut() => Report(Stopped());

        private Settled<TResult> Stopped() =>
            timer is { IsCancellationRequested: true } && !siblings.IsCancellationRequested
                ? new(
                    BranchStatus.TimedOut,
                    default,
                    NodeException.Transient(
                        $"{settings.Noun.One} '{name}' timed out after {(long)settings.Timeout!.Value.TotalMilliseconds} ms"),
                    null)
                : new(BranchStatus.Cancelled, default, null, null);

        private void Report(Settled<TResult> settled)
        {
            if (Interlocked.Exchange(ref _reported, 1) == 0)
            {
                inbox.Post(index, settled);
            }
        }
    }

    /// <summary>
    /// Where a fan-out's branches report how they settled, in the order they did, and what its loop waits on
    /// (<c>design/49-Burst-Throughput.md</c>). Each branch reports once, so it holds at most one report per
    /// branch. The loop waits without allocating, on a source it resets each time; the attempt's token wakes
    /// it through one registration for the whole fan-out.
    /// </summary>
    private sealed class Inbox<TResult>(int capacity) : IValueTaskSource
    {
        private readonly Lock _gate = new();
        private readonly (int Index, Settled<TResult> Settled)[] _reports = new (int, Settled<TResult>)[capacity];
        private int _head;
        private int _tail;
        private bool _waiting;
        private bool _cancelled;

        // A report wakes the loop on the reporting thread, as a completed task's continuation would.
        private ManualResetValueTaskSourceCore<bool> _wait;

        public void Post(int index, Settled<TResult> settled)
        {
            bool wake;
            lock (_gate)
            {
                _reports[_tail++] = (index, settled);
                wake = _waiting;
                _waiting = false;
            }

            if (wake)
            {
                _wait.SetResult(true);
            }
        }

        /// <summary>
        /// The attempt's token fired. The loop is woken on the thread pool, not inside whatever cancelled the
        /// token, since it goes on to end the node and maybe the run.
        /// </summary>
        public void Cancel()
        {
            bool wake;
            lock (_gate)
            {
                _cancelled = true;
                wake = _waiting;
                _waiting = false;
            }

            if (wake)
            {
                ThreadPool.UnsafeQueueUserWorkItem(static inbox => inbox._wait.SetResult(true), this, preferLocal: false);
            }
        }

        /// <summary>
        /// Completes once a report is waiting, or, when <paramref name="cancellable"/>, once the attempt's token
        /// has fired. Only the fan-out's loop calls it, never twice at once.
        /// </summary>
        public ValueTask WaitAsync(bool cancellable)
        {
            lock (_gate)
            {
                if (_head != _tail || (cancellable && _cancelled))
                {
                    return ValueTask.CompletedTask;
                }

                _wait.Reset();
                _waiting = true;
                return new ValueTask(this, _wait.Version);
            }
        }

        public bool TryTake(out int index, out Settled<TResult> settled)
        {
            lock (_gate)
            {
                if (_head == _tail)
                {
                    (index, settled) = (-1, default);
                    return false;
                }

                (index, settled) = _reports[_head];
                _reports[_head++] = default;
                return true;
            }
        }

        void IValueTaskSource.GetResult(short token) => _wait.GetResult(token);

        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _wait.GetStatus(token);

        void IValueTaskSource.OnCompleted(
            Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _wait.OnCompleted(continuation, state, token, flags);
    }
}
