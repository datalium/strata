using Datalium.Strata.Graph;

namespace Datalium.Strata.Observability;

/// <summary>
/// Caller metadata on checkpoints (<c>design/18-Run-Observability.md</c>, SG-14). It shares the
/// checkpoint's <c>metadata</c> map with the engine's own keys, so those names, and every key under
/// the <c>strata.</c> prefix kept for future engine keys, are refused. Mirrors the Rust engine's
/// <c>context::run_metadata</c>.
/// </summary>
internal static class RunMetadata
{
    /// <summary>Kept for engine keys a later version adds, so they never collide with a caller's.</summary>
    public const string ReservedPrefix = "strata.";

    private static readonly HashSet<string> EngineKeys = ["error", "error_kind", "attempts", "cancel_reason"];

    public static readonly IReadOnlyDictionary<string, string> None = new Dictionary<string, string>();

    public static bool IsReserved(string key) =>
        EngineKeys.Contains(key) || key.StartsWith(ReservedPrefix, StringComparison.Ordinal);

    /// <summary>Refuses options a run can't start with, before anything is loaded or written.</summary>
    public static void Validate(RunOptions options)
    {
        foreach (var key in options.Metadata.Keys)
        {
            if (key.Length == 0)
            {
                throw new ArgumentException("run metadata keys can't be empty", nameof(options));
            }

            if (IsReserved(key))
            {
                throw new ArgumentException($"run metadata key '{key}' is reserved", nameof(options));
            }
        }

        if (options.RunId is { Length: 0 })
        {
            throw new ArgumentException("a run id can't be empty", nameof(options));
        }

        if (options.MaxNodeHops is < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), options.MaxNodeHops, "a run's MaxNodeHops must be at least 1");
        }

        if (options.Lease is { } lease)
        {
            if (string.IsNullOrWhiteSpace(lease.Owner))
            {
                throw new ArgumentException("a run's lease needs an owner", nameof(options));
            }

            if (lease.Ttl < Interruption.ThreadLeases.MinimumTtl)
            {
                throw new ArgumentException("a run's lease ttl must be at least 1 millisecond", nameof(options));
            }
        }
    }

    /// <summary>A copy of the caller's metadata, so changing their dictionary later has no effect.</summary>
    public static IReadOnlyDictionary<string, string> Snapshot(Dictionary<string, string> metadata) =>
        metadata.Count == 0 ? None : new Dictionary<string, string>(metadata);

    /// <summary>
    /// A resume's metadata: the caller keys of the checkpoint it resumes, with the resume's own laid
    /// over them key by key.
    /// </summary>
    public static IReadOnlyDictionary<string, string> Resumed(
        IReadOnlyDictionary<string, string>? stored, IReadOnlyDictionary<string, string> given)
    {
        if (stored is null || !stored.Keys.Any(key => !IsReserved(key)))
        {
            return given;
        }

        var merged = stored.Where(entry => !IsReserved(entry.Key)).ToDictionary();
        foreach (var (key, value) in given)
        {
            merged[key] = value;
        }

        return merged;
    }

    /// <summary>
    /// What a checkpoint's <c>metadata</c> holds: the run's keys and <paramref name="engine"/>'s, which
    /// can't collide. <see langword="null"/> when both are empty, as a checkpoint has always had.
    /// </summary>
    public static Dictionary<string, string>? ForCheckpoint(
        IReadOnlyDictionary<string, string> run, Dictionary<string, string>? engine)
    {
        if (run.Count == 0)
        {
            return engine;
        }

        var metadata = new Dictionary<string, string>(run);
        foreach (var (key, value) in engine ?? [])
        {
            metadata[key] = value;
        }

        return metadata;
    }
}
