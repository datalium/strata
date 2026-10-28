namespace Datalium.Strata.Graph;

/// <summary>
/// The rules for node and branch metadata (<c>design/22-Graph-Structure-In-Definitions.md</c>), shared
/// by <see cref="StateGraph{TState}"/> and <see cref="ParallelBranches{TState}"/>. Mirrors Rust's
/// <c>check_metadata_keys</c> in <c>graph.rs</c>, messages included.
/// </summary>
internal static class NodeMetadata
{
    /// <summary>The key prefix node and branch metadata may not use: it is kept for the engine.</summary>
    internal const string ReservedPrefix = "strata.";

    /// <summary>A copy, so changing the caller's dictionary afterwards can't change the graph.</summary>
    internal static IReadOnlyDictionary<string, string> Copy(IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        var copy = new Dictionary<string, string>(metadata.Count);
        foreach (var (key, value) in metadata)
        {
            if (value is null)
            {
                throw new ArgumentException($"metadata key '{key}' has a null value", nameof(metadata));
            }

            copy[key] = value;
        }

        return copy;
    }

    /// <summary>
    /// Refuses an empty key or one starting <c>strata.</c>, checking keys in ordinal order so the
    /// first error reported is the one Rust reports.
    /// </summary>
    internal static void CheckKeys(IReadOnlyDictionary<string, string> metadata, string owner)
    {
        foreach (var key in metadata.Keys.Order(StringComparer.Ordinal))
        {
            if (key.Length == 0)
            {
                throw new InvalidOperationException($"metadata on {owner} has an empty key");
            }

            if (key.StartsWith(ReservedPrefix, StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"metadata key '{key}' on {owner} is reserved");
            }
        }
    }

    /// <summary><paramref name="metadata"/> with its keys in ordinal order, as both engines export them.</summary>
    internal static IReadOnlyDictionary<string, string> Sorted(IReadOnlyDictionary<string, string> metadata)
    {
        var sorted = new Dictionary<string, string>(metadata.Count);
        foreach (var key in metadata.Keys.Order(StringComparer.Ordinal))
        {
            sorted[key] = metadata[key];
        }

        return sorted;
    }
}
