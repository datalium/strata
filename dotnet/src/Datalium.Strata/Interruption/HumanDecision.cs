using System.Text.Json;
using System.Text.Json.Nodes;

namespace Datalium.Strata.Interruption;

/// <summary>
/// A <see cref="HumanSignal"/> as the engine recorded it, when it answered an interrupt boundary
/// (<c>design/21-Typed-Human-Signals.md</c>). Every node of that graph level sees it
/// (<c>NodeContext.Decision</c>) until another boundary of the level is answered, and every checkpoint
/// the level writes meanwhile carries it, so a failed thread resumed later still knows who approved.
/// Resuming a failed or cancelled thread doesn't replace it. Mirrors the Rust engine's
/// <c>HumanDecision</c> and the checkpoint's <c>decision</c> field.
/// </summary>
public sealed record HumanDecision
{
    private static readonly IReadOnlyDictionary<string, JsonElement> NoOverrides =
        new SortedDictionary<string, JsonElement>(StringComparer.Ordinal);

    /// <summary>The interrupt boundary it answered, a node of its own level.</summary>
    public required string Boundary { get; init; }

    /// <summary>The signal's <see cref="HumanSignal.IsApproved"/>.</summary>
    public required bool IsApproved { get; init; }

    /// <summary>The signal's <see cref="HumanSignal.ReviewerToken"/>: who answered.</summary>
    public required string Reviewer { get; init; }

    /// <summary>
    /// Every override the signal applied, sorted by key: typed ones with their JSON type, string ones
    /// as JSON strings. Immutable, so concurrent branches can share it.
    /// </summary>
    public IReadOnlyDictionary<string, JsonElement> Overrides { get; init; } = NoOverrides;

    /// <summary>When the resume that answered began, as an RFC 3339 string.</summary>
    public required string DecidedAt { get; init; }

    /// <summary>The decision <paramref name="signal"/> makes when it answers <paramref name="boundary"/>.</summary>
    internal static HumanDecision From(HumanSignal signal, string boundary, string decidedAt) => new()
    {
        Boundary = boundary,
        IsApproved = signal.IsApproved,
        Reviewer = signal.ReviewerToken,
        Overrides = Freeze(signal.EffectiveOverrides(out _)),
        DecidedAt = decidedAt,
    };

    /// <summary>This decision as the checkpoint's <c>decision</c> JSON.</summary>
    public JsonObject ToWireJson()
    {
        var overrides = new JsonObject();
        foreach (var (key, value) in Overrides.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            overrides[key] = JsonSerializer.SerializeToNode(value);
        }

        return new JsonObject
        {
            ["boundary"] = Boundary,
            ["is_approved"] = IsApproved,
            ["reviewer"] = Reviewer,
            ["overrides"] = overrides,
            ["decided_at"] = DecidedAt,
        };
    }

    /// <summary>The inverse of <see cref="ToWireJson"/>.</summary>
    public static HumanDecision FromWireJson(JsonNode node)
    {
        var json = node.AsObject();
        var overrides = new SortedDictionary<string, JsonNode?>(StringComparer.Ordinal);
        if (json["overrides"] is JsonObject stored)
        {
            foreach (var (key, value) in stored)
            {
                overrides[key] = value?.DeepClone();
            }
        }

        return new HumanDecision
        {
            Boundary = json["boundary"]!.GetValue<string>(),
            IsApproved = json["is_approved"]!.GetValue<bool>(),
            Reviewer = json["reviewer"]!.GetValue<string>(),
            Overrides = Freeze(overrides),
            DecidedAt = json["decided_at"]!.GetValue<string>(),
        };
    }

    private static SortedDictionary<string, JsonElement> Freeze(SortedDictionary<string, JsonNode?> overrides)
    {
        var frozen = new SortedDictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var (key, value) in overrides)
        {
            frozen[key] = JsonSerializer.SerializeToElement(value);
        }

        return frozen;
    }
}
