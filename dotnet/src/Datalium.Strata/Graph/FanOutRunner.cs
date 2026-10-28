using System.Diagnostics;
using System.Runtime.ExceptionServices;
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
/// and its own token (the siblings' token plus its own timeout).
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
    /// <summary>How one branch settled, as its wrapper task reports it: it never throws.</summary>
    private sealed record Settled<TResult>(BranchStatus Status, TResult? State, NodeException? Error, Exception? Bug);

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
        var sources = new List<CancellationTokenSource>();
        var outcomes = new BranchOutcome<TResult>?[count];
        var scopes = new BranchScope?[count];
        var running = new Dictionary<Task<Settled<TResult>>, int>();
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
            while (running.Count > 0)
            {
                var done = await Task.WhenAny(running.Keys).WaitAsync(inputs.Token).ConfigureAwait(false);
                inputs.Token.ThrowIfCancellationRequested();
                running.Remove(done, out var index);
                Record(index, done.Result);

                if (!decided && counted >= needed)
                {
                    decided = met = true;
                    Decide(met: true);
                    await GiveUpAsync(BranchEndReason.LostRace).ConfigureAwait(false);
                    break;
                }

                if (!decided && counted + running.Count + (count - next) < needed)
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
            foreach (var source in sources)
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
            while (startMore && next < count && running.Count < limit)
            {
                var index = next++;
                running.Add(Start(index), index);
            }
        }

        // Starts one branch inside its own span, which the launcher (and a sync handler's thread) sees
        // as current; the fan-out's span is current again once it has started. Its token is the
        // siblings' one, plus its own timeout on the run's clock when there is one.
        Task<Settled<TResult>> Start(int index)
        {
            CancellationTokenSource? timer = null;
            CancellationTokenSource own;
            if (settings.Timeout is { } timeout)
            {
                timer = new CancellationTokenSource(timeout, inputs.Level.Run.Clock);
                sources.Add(timer);
                own = CancellationTokenSource.CreateLinkedTokenSource(siblings.Token, timer.Token);
            }
            else
            {
                own = CancellationTokenSource.CreateLinkedTokenSource(siblings.Token);
            }

            sources.Add(own);
            var path = $"{inputs.Context.NodePath}/{names[index]}";
            var scope = inputs.Level.Run.Telemetry.BranchStarted(path, inputs.Context.Attempt);
            scopes[index] = scope;
            Task<TResult> task;
            try
            {
                task = launch(index, new BranchLaunch(path, scope.Activity, own.Token));
            }
            catch (Exception ex)
            {
                task = Task.FromException<TResult>(ex);
            }
            finally
            {
                Activity.Current = groupActivity;
            }

            return Settle(names[index], task, own.Token, timer, siblings.Token, settings);
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
            if (settings.WindsDown && running.Count > 0)
            {
                // Settle never throws.
                await Task.WhenAll(running.Keys).ConfigureAwait(false);
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

        void RecordSettled()
        {
            foreach (var (task, index) in running.Where(r => r.Key.IsCompleted).ToList())
            {
                running.Remove(task);
                if (task.Result.Bug is null)
                {
                    Record(index, task.Result);
                }
            }
        }

        void ReportCancelled(BranchEndReason? reason)
        {
            foreach (var index in running.Values)
            {
                scopes[index]!.Completed(BranchResult.Cancelled, error: null, reason);
                outcomes[index] = new BranchOutcome<TResult> { Name = names[index], Status = BranchStatus.Cancelled };
            }

            running.Clear();
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
    /// Waits for one branch, resolved to how it settled. A branch that is abandoned stops being waited
    /// for the moment its token fires; one that winds down is waited for until it has stopped, and
    /// counts as stopped by its token if it didn't succeed. Either way, its own timeout makes it
    /// <see cref="BranchStatus.TimedOut"/> (a transient failure), and anything else that fired its token
    /// <see cref="BranchStatus.Cancelled"/>.
    /// </summary>
    private static async Task<Settled<TResult>> Settle<TResult>(
        string name,
        Task<TResult> task,
        CancellationToken token,
        CancellationTokenSource? timer,
        CancellationToken siblings,
        FanOutSettings<TResult> settings)
    {
        try
        {
            var result = settings.WindsDown
                ? await task.ConfigureAwait(false)
                : await task.WaitAsync(token).ConfigureAwait(false);
            return new(BranchStatus.Succeeded, result, null, null);
        }
        catch (Exception ex) when (token.IsCancellationRequested && (settings.WindsDown || ex is OperationCanceledException))
        {
            if (!task.IsCompleted)
            {
                NodeHandlers.Abandon(task);
            }

            return timer is { IsCancellationRequested: true } && !siblings.IsCancellationRequested
                ? new(
                    BranchStatus.TimedOut,
                    default,
                    NodeException.Transient(
                        $"{settings.Noun.One} '{name}' timed out after {(long)settings.Timeout!.Value.TotalMilliseconds} ms"),
                    null)
                : new(BranchStatus.Cancelled, default, null, null);
        }
        catch (NodeException ex)
        {
            return new(BranchStatus.Failed, default, ex, null);
        }
        catch (Exception ex)
        {
            // Not a NodeException, so not recovery-managed: propagate like a plain node's bug would.
            return new(BranchStatus.Failed, default, null, ex);
        }
    }
}
