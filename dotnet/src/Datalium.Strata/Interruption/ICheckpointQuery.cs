using System.Buffers.Text;
using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Datalium.Strata.Interruption;

/// <summary>
/// Lists threads and reads a thread's checkpoint log (<c>design/19-Async-Queryable-Store.md</c>,
/// "Query port"). Separate from <see cref="ICheckpointStore"/> so a store that can only save and load
/// stays complete; every adapter in <c>Datalium.Strata.Storage</c> implements both. Mirrors
/// <c>rust/strata-core/src/store.rs</c>'s <c>CheckpointQuery</c>.
/// </summary>
public interface ICheckpointQuery
{
    /// <summary>
    /// One page of threads matching <paramref name="query"/>, most recently updated first. A page may
    /// hold fewer than <see cref="ThreadQuery.Limit"/> threads even when more follow: only a
    /// <c>null</c> <see cref="ThreadPage.NextCursor"/> means the end.
    /// </summary>
    ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Up to <paramref name="limit"/> (1 to <see cref="ThreadQuery.MaxLimit"/>) of
    /// <paramref name="threadId"/>'s checkpoints with a sequence above <paramref name="afterSequence"/>,
    /// oldest first. An unknown thread gives an empty page.
    /// </summary>
    ValueTask<CheckpointLogPage> GetLogAsync(
        string threadId,
        long? afterSequence = null,
        int limit = ThreadQuery.DefaultLimit,
        CancellationToken cancellationToken = default);
}

/// <summary>Where a thread stands, derived from its latest checkpoint.</summary>
public enum CheckpointThreadState
{
    /// <summary>Waiting for a human: the latest checkpoint is interrupted and was not resumed.</summary>
    Interrupted,

    /// <summary>Stopped by a node failure, not resumed.</summary>
    Failed,

    /// <summary>Stopped by cancellation or a deadline, not resumed.</summary>
    Cancelled,

    /// <summary>
    /// A resume began from the latest checkpoint, by a graph that doesn't checkpoint every node. The resumed
    /// run may still be running, may have completed (which writes nothing), or may have died with its
    /// process: the store can't tell which. A graph compiled with <c>CheckpointPolicy.EveryNode</c> never
    /// leaves this state: its resume makes the thread <see cref="Running"/>
    /// (<c>design/33-Durable-Execution.md</c>).
    /// </summary>
    Resumed,

    /// <summary>
    /// The latest checkpoint is completed: a run of a graph compiled with <c>CheckpointPolicy.EveryNode</c>
    /// finished (<c>design/33-Durable-Execution.md</c>). Other graphs write nothing when they complete.
    /// </summary>
    Completed,

    /// <summary>
    /// The thread was ended where it stood (<c>design/28-Ending-A-Thread.md</c>): it will not run again.
    /// </summary>
    Ended,

    /// <summary>
    /// The latest checkpoint is running (<c>design/33-Durable-Execution.md</c>): a run of a graph compiled
    /// with <c>CheckpointPolicy.EveryNode</c> is going on, or its process died. With leases, a running thread
    /// no live lease holds is dead, and <c>CompiledGraph.RecoverStaleAsync</c> recovers it.
    /// </summary>
    Running,
}

/// <summary>Helpers for <see cref="CheckpointThreadState"/>, matching the Rust engine's.</summary>
public static class CheckpointThreadStates
{
    /// <summary>Every state, in the order the Redis index script expects its sets.</summary>
    public static IReadOnlyList<CheckpointThreadState> All { get; } =
        [
            CheckpointThreadState.Interrupted,
            CheckpointThreadState.Failed,
            CheckpointThreadState.Cancelled,
            CheckpointThreadState.Resumed,
            CheckpointThreadState.Completed,
            CheckpointThreadState.Ended,
            CheckpointThreadState.Running,
        ];

    /// <summary>The state of a thread whose latest checkpoint is <paramref name="checkpoint"/>.</summary>
    public static CheckpointThreadState Of(Checkpoint checkpoint) =>
        checkpoint.ResumedAt is not null
            ? CheckpointThreadState.Resumed
            : checkpoint.Status switch
            {
                CheckpointStatus.Interrupted => CheckpointThreadState.Interrupted,
                CheckpointStatus.Failed => CheckpointThreadState.Failed,
                CheckpointStatus.Cancelled => CheckpointThreadState.Cancelled,
                CheckpointStatus.Completed => CheckpointThreadState.Completed,
                CheckpointStatus.Ended => CheckpointThreadState.Ended,
                CheckpointStatus.Running => CheckpointThreadState.Running,
                _ => throw new ArgumentOutOfRangeException(nameof(checkpoint), checkpoint.Status, "unknown checkpoint status"),
            };

    /// <summary>The name stored in the SQL head table and the Redis index keys.</summary>
    public static string Name(this CheckpointThreadState state) => state switch
    {
        CheckpointThreadState.Interrupted => "interrupted",
        CheckpointThreadState.Failed => "failed",
        CheckpointThreadState.Cancelled => "cancelled",
        CheckpointThreadState.Resumed => "resumed",
        CheckpointThreadState.Completed => "completed",
        CheckpointThreadState.Ended => "ended",
        CheckpointThreadState.Running => "running",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "unknown thread state"),
    };

    /// <summary>The inverse of <see cref="Name"/>.</summary>
    public static CheckpointThreadState Parse(string name)
    {
        foreach (var state in All)
        {
            if (state.Name() == name)
            {
                return state;
            }
        }

        throw new ArgumentException($"unknown thread state '{name}'", nameof(name));
    }
}

/// <summary>Which threads <see cref="ICheckpointQuery.ListThreadsAsync"/> returns.</summary>
public sealed record ThreadQuery
{
    /// <summary>The page size a query starts with.</summary>
    public const int DefaultLimit = 100;

    /// <summary>The most threads or log entries one page may ask for.</summary>
    public const int MaxLimit = 1000;

    private static readonly IReadOnlyDictionary<string, string> NoMetadata = new Dictionary<string, string>();

    /// <summary>Only threads in one of these states; <c>null</c> or empty means any state.</summary>
    public IReadOnlySet<CheckpointThreadState>? States { get; init; }

    /// <summary>Only threads whose latest checkpoint's metadata has every pair, the engine's keys included.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = NoMetadata;

    /// <summary>Only threads updated strictly after this, at second precision.</summary>
    public DateTimeOffset? UpdatedAfter { get; init; }

    /// <summary>Only threads updated strictly before this, at second precision.</summary>
    public DateTimeOffset? UpdatedBefore { get; init; }

    /// <summary>
    /// Only threads due at this time (<c>design/29-Event-Waits-And-Timers.md</c>): waiting (interrupted, not
    /// resumed), with an innermost <c>wait.wake_at</c> at or before it, at second precision.
    /// </summary>
    public DateTimeOffset? DueBy { get; init; }

    /// <summary>
    /// Only threads whose latest checkpoint's outermost level was written by a graph with this id
    /// (<c>design/31-Graph-Identity.md</c>). A thread with no identity never matches.
    /// </summary>
    public string? GraphId { get; init; }

    /// <summary>Only threads written by this version of <see cref="GraphId"/>, which it needs.</summary>
    public string? GraphVersion { get; init; }

    /// <summary>How many threads a page holds at most: 1 to <see cref="MaxLimit"/>.</summary>
    public int Limit { get; init; } = DefaultLimit;

    /// <summary>Where to continue: the previous page's <see cref="ThreadPage.NextCursor"/>.</summary>
    public string? Cursor { get; init; }

    /// <summary>
    /// Refuses a limit out of range, a malformed cursor, or a <see cref="GraphVersion"/> without a
    /// <see cref="GraphId"/>; returns the decoded cursor.
    /// </summary>
    public ThreadCursor? Validate()
    {
        CheckLimit(Limit);
        if (GraphVersion is not null && GraphId is null)
        {
            throw new ArgumentException("a thread query's GraphVersion needs a GraphId", nameof(GraphVersion));
        }

        return Cursor is null ? null : ThreadCursor.Decode(Cursor);
    }

    /// <summary><see cref="UpdatedAfter"/> as stored text, for comparing with <c>updated_at</c>.</summary>
    public string? UpdatedAfterText => UpdatedAfter is { } after ? CheckpointTimestamps.Text(after) : null;

    /// <summary><see cref="UpdatedBefore"/> as stored text, for comparing with <c>updated_at</c>.</summary>
    public string? UpdatedBeforeText => UpdatedBefore is { } before ? CheckpointTimestamps.Text(before) : null;

    /// <summary><see cref="DueBy"/> as stored text, for comparing with <c>wake_at</c>.</summary>
    public string? DueByText => DueBy is { } due ? CheckpointTimestamps.Text(due) : null;

    /// <summary>
    /// Whether a thread whose latest checkpoint is <paramref name="checkpoint"/> matches every filter
    /// (not the cursor). For stores that filter in code.
    /// </summary>
    public bool Matches(Checkpoint checkpoint)
    {
        if (States is { Count: > 0 } states && !states.Contains(CheckpointThreadStates.Of(checkpoint)))
        {
            return false;
        }

        if (Metadata.Count > 0)
        {
            if (checkpoint.Metadata is not { } metadata)
            {
                return false;
            }

            foreach (var (key, value) in Metadata)
            {
                if (!metadata.TryGetValue(key, out var actual) || actual != value)
                {
                    return false;
                }
            }
        }

        if (DueByText is { } due && !IsDue(checkpoint, due))
        {
            return false;
        }

        if (GraphId is not null
            && (checkpoint.Graph is not { } graph || graph.Id != GraphId
                || (GraphVersion is not null && graph.Version != GraphVersion)))
        {
            return false;
        }

        var updated = CheckpointTimestamps.UpdatedAt(checkpoint);
        if (UpdatedAfterText is { } after && string.CompareOrdinal(updated, after) <= 0)
        {
            return false;
        }

        return UpdatedBeforeText is not { } before || string.CompareOrdinal(updated, before) < 0;
    }

    /// <summary>
    /// Whether a thread whose latest checkpoint is <paramref name="checkpoint"/> is due at
    /// <paramref name="due"/> (stored text): waiting, not resumed, with an innermost <c>wake_at</c> at or
    /// before it (<c>design/29-Event-Waits-And-Timers.md</c>).
    /// </summary>
    public static bool IsDue(Checkpoint checkpoint, string due) =>
        CheckpointThreadStates.Of(checkpoint) == CheckpointThreadState.Interrupted
        && checkpoint.InnermostWait()?.WakeAt is { } wakeAt
        && string.CompareOrdinal(wakeAt, due) <= 0;

    /// <summary>Refuses a page size outside 1 to <see cref="MaxLimit"/>.</summary>
    public static void CheckLimit(int limit)
    {
        if (limit is < 1 or > MaxLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), limit, $"a page size must be between 1 and {MaxLimit}");
        }
    }
}

/// <summary>A listed thread: enough to act on without loading its checkpoint.</summary>
/// <param name="Sequence">The latest checkpoint's sequence in the thread's log.</param>
/// <param name="Status">The latest checkpoint's own status.</param>
/// <param name="WaitingAt">As <see cref="Checkpoint.WaitingAt"/>.</param>
/// <param name="Metadata">The latest checkpoint's whole metadata map, the engine's keys included.</param>
public sealed record ThreadSummary(
    string ThreadId,
    long Sequence,
    CheckpointThreadState State,
    CheckpointStatus Status,
    string? SuspendedNode,
    string? NextNode,
    IReadOnlyList<string> WaitingAt,
    IReadOnlyDictionary<string, string> Metadata,
    DateTimeOffset UpdatedAt,
    string SchemaVersion)
{
    /// <summary>
    /// What the latest checkpoint's innermost level waits for, and until when
    /// (<c>design/29-Event-Waits-And-Timers.md</c>): <see cref="Checkpoint.InnermostWait"/>.
    /// </summary>
    public CheckpointWait? Wait { get; init; }

    /// <summary>
    /// Which graph wrote the latest checkpoint's outermost level (<c>design/31-Graph-Identity.md</c>), or
    /// <see langword="null"/> when it has no identity.
    /// </summary>
    public GraphIdentity? Graph { get; init; }

    /// <summary>The summary of a thread whose latest checkpoint, at <paramref name="sequence"/>, is <paramref name="checkpoint"/>.</summary>
    public static ThreadSummary Of(long sequence, Checkpoint checkpoint) => new(
        checkpoint.ThreadId,
        sequence,
        CheckpointThreadStates.Of(checkpoint),
        checkpoint.Status,
        checkpoint.SuspendedNode,
        checkpoint.NextNode,
        checkpoint.WaitingAt(),
        checkpoint.Metadata ?? new Dictionary<string, string>(),
        CheckpointTimestamps.Parse(CheckpointTimestamps.UpdatedAt(checkpoint)),
        checkpoint.SchemaVersion)
    {
        Wait = checkpoint.InnermostWait(),
        Graph = checkpoint.Graph,
    };
}

/// <summary>One page of <see cref="ICheckpointQuery.ListThreadsAsync"/>.</summary>
/// <param name="NextCursor">Pass this as <see cref="ThreadQuery.Cursor"/> for the next page; <c>null</c> at the end.</param>
public sealed record ThreadPage(IReadOnlyList<ThreadSummary> Threads, string? NextCursor);

/// <summary>One checkpoint of a thread's log, with its sequence.</summary>
public sealed record CheckpointLogEntry(long Sequence, Checkpoint Checkpoint);

/// <summary>One page of <see cref="ICheckpointQuery.GetLogAsync"/>.</summary>
/// <param name="NextAfterSequence">The last entry's sequence when the page was full (more may follow); <c>null</c> otherwise.</param>
public sealed record CheckpointLogPage(IReadOnlyList<CheckpointLogEntry> Entries, long? NextAfterSequence)
{
    /// <summary>The page for <paramref name="entries"/> read with <paramref name="limit"/>.</summary>
    public static CheckpointLogPage For(IReadOnlyList<CheckpointLogEntry> entries, int limit) =>
        new(entries, entries.Count == limit ? entries[^1].Sequence : null);
}

/// <summary>
/// The position a thread listing continues from: the last returned thread's stored
/// <c>updated_at</c> and id. Encoded as base64url (no padding) of the JSON array
/// <c>[updated_at, thread_id]</c>, as the Rust engine does (<c>schema/README.md</c>, "Thread listing
/// cursor"), so a cursor from either engine decodes in the other.
/// </summary>
public sealed record ThreadCursor(string UpdatedAt, string ThreadId)
{
    private static readonly JsonSerializerOptions Compact = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public string Encode() =>
        Base64Url.EncodeToString(JsonSerializer.SerializeToUtf8Bytes(new[] { UpdatedAt, ThreadId }, Compact));

    public static ThreadCursor Decode(string cursor)
    {
        try
        {
            var parts = JsonSerializer.Deserialize<string[]>(Base64Url.DecodeFromChars(cursor));
            if (parts is [var updatedAt, var threadId])
            {
                return new ThreadCursor(updatedAt, threadId);
            }
        }
        catch (Exception e) when (e is FormatException or JsonException)
        {
        }

        throw new ArgumentException($"'{cursor}' is not a thread listing cursor", nameof(cursor));
    }

    /// <summary>
    /// Whether a thread at (<paramref name="updatedAt"/>, <paramref name="threadId"/>) comes after this
    /// cursor in listing order (newest first, then thread id descending, both ordinal), so belongs on
    /// a page that continues from it.
    /// </summary>
    public bool Admits(string updatedAt, string threadId)
    {
        var byTime = string.CompareOrdinal(updatedAt, UpdatedAt);
        return byTime < 0 || (byTime == 0 && string.CompareOrdinal(threadId, ThreadId) < 0);
    }
}

/// <summary>How the engines write and compare checkpoint timestamps.</summary>
public static class CheckpointTimestamps
{
    /// <summary>
    /// When a thread whose latest checkpoint is <paramref name="checkpoint"/> was last written, as
    /// stored: <c>resumed_at</c> if set (the resume stamp keeps the original <c>created_at</c>), else
    /// <c>created_at</c>.
    /// </summary>
    public static string UpdatedAt(Checkpoint checkpoint) => checkpoint.ResumedAt ?? checkpoint.CreatedAt;

    /// <summary><paramref name="time"/> the way the engines write timestamps: RFC 3339, whole seconds, <c>Z</c>.</summary>
    public static string Text(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);

    /// <summary>Parses a stored timestamp.</summary>
    public static DateTimeOffset Parse(string text) =>
        DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var time)
            ? time.ToUniversalTime()
            : throw new InvalidOperationException($"bad timestamp '{text}' in the checkpoint log");
}
