using Datalium.Strata.Interruption;

namespace Datalium.Strata.Storage;

/// <summary>
/// Plumbing the adapters share for the query port (<c>design/19-Async-Queryable-Store.md</c>): the
/// stored status names, and the in-code listing the in-memory and Redis stores use. Mirrors
/// <c>strata-memory</c>'s <c>support.rs</c>.
/// </summary>
internal static class CheckpointListing
{
    public static string StatusToString(CheckpointStatus status) => status switch
    {
        CheckpointStatus.Interrupted => "interrupted",
        CheckpointStatus.Completed => "completed",
        CheckpointStatus.Failed => "failed",
        CheckpointStatus.Cancelled => "cancelled",
        CheckpointStatus.Ended => "ended",
        CheckpointStatus.Running => "running",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown checkpoint status"),
    };

    public static CheckpointStatus StatusFromString(string status) => status switch
    {
        "interrupted" => CheckpointStatus.Interrupted,
        "completed" => CheckpointStatus.Completed,
        "failed" => CheckpointStatus.Failed,
        "cancelled" => CheckpointStatus.Cancelled,
        "ended" => CheckpointStatus.Ended,
        "running" => CheckpointStatus.Running,
        _ => throw new InvalidOperationException($"unknown checkpoint status '{status}' in the checkpoint log"),
    };

    /// <summary>
    /// One page of <paramref name="query"/> over every thread's latest checkpoint (with its
    /// sequence), for stores that list in code. Exact: the cursor is <c>null</c> only when nothing more
    /// matches.
    /// </summary>
    public static ThreadPage PageFromLatest(IEnumerable<(long Sequence, Checkpoint Checkpoint)> latest, ThreadQuery query)
    {
        var cursor = query.Validate();
        var matching = latest
            .Where(t => query.Matches(t.Checkpoint))
            .Where(t => cursor is null || cursor.Admits(CheckpointTimestamps.UpdatedAt(t.Checkpoint), t.Checkpoint.ThreadId))
            .OrderByDescending(t => CheckpointTimestamps.UpdatedAt(t.Checkpoint), StringComparer.Ordinal)
            .ThenByDescending(t => t.Checkpoint.ThreadId, StringComparer.Ordinal)
            .Take(query.Limit + 1)
            .ToList();

        var more = matching.Count > query.Limit;
        var page = matching.Take(query.Limit).ToList();
        var next = more
            ? new ThreadCursor(CheckpointTimestamps.UpdatedAt(page[^1].Checkpoint), page[^1].Checkpoint.ThreadId).Encode()
            : null;
        return new ThreadPage([.. page.Select(t => ThreadSummary.Of(t.Sequence, t.Checkpoint))], next);
    }
}
