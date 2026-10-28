using System.Globalization;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Graph;

/// <summary>
/// The hop count's place on the wire (<c>design/24-Loop-Circuit-Breaker.md</c>, "Persistence"): an
/// engine key in a checkpoint's <c>metadata</c>, holding the hops a level made before the checkpoint's
/// next node, which is the count a resume from it starts from. Mirrors the Rust engine's
/// <c>checkpoint::HOPS_KEY</c>.
/// </summary>
internal static class HopLimit
{
    public const string MetadataKey = "strata.hops";

    /// <summary>
    /// The count <paramref name="checkpoint"/>'s level resumes from: its <c>strata.hops</c>, or 0 when it
    /// has none (written before SG-05, or while the level had no limit). <see langword="false"/>, with the
    /// stored value in <paramref name="invalid"/>, when the value is not a non-negative integer.
    /// </summary>
    public static bool TryRead(Checkpoint checkpoint, out int hops, out string? invalid)
    {
        hops = 0;
        invalid = null;
        if (checkpoint.Metadata?.TryGetValue(MetadataKey, out var stored) is not true)
        {
            return true;
        }

        if (int.TryParse(stored, NumberStyles.None, CultureInfo.InvariantCulture, out hops))
        {
            return true;
        }

        invalid = stored;
        return false;
    }

    public static string Format(int hops) => hops.ToString(CultureInfo.InvariantCulture);
}
