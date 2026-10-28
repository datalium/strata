using System.Text.Json.Nodes;

namespace Datalium.Strata.Interruption;

/// <summary>
/// Human approval data injected on resume (STRATA-SPEC-002's <c>HumanFeedback</c>/
/// <c>HumanSignal</c>). Its overrides are merged into the frozen state's JSON object before it's
/// rehydrated back into <c>TState</c>: <see cref="FieldOverrides"/> as JSON strings, and
/// <see cref="Overrides"/> with their JSON type (<c>design/21-Typed-Human-Signals.md</c>).
///
/// A resume that answers an interrupt boundary records the signal as the level's
/// <see cref="HumanDecision"/>, which every node after it sees (<c>NodeContext.Decision</c>) and every
/// checkpoint the level writes afterwards carries. A boundary added with a reject route continues
/// there when <see cref="IsApproved"/> is <see langword="false"/>.
///
/// Resuming a <c>failed</c> or <c>cancelled</c> thread takes a signal too, but uses only its overrides:
/// no decision is recorded, no reject route is taken, and the level keeps the decision it already had,
/// so a retry keeps the original approver. <see cref="IsApproved"/> and <see cref="ReviewerToken"/> mean
/// nothing there. By convention, pass <c>HumanSignal.Approve(who)</c> with the overrides. An outer level's
/// entry in <c>LevelSignals</c> is the same (<c>design/16-Nested-Interrupts.md</c>, "Resuming a failed thread").
/// </summary>
/// <param name="IsApproved">The human's answer. At a boundary with a reject route, <see langword="false"/> takes the route.</param>
/// <param name="ReviewerToken">
/// Who answered: an identity such as a user id or an email address, never a credential. The engine
/// persists it in checkpoints and shows it to every node and observer.
/// </param>
/// <param name="FieldOverrides">Overrides written as JSON strings, whatever the field's type.</param>
public sealed record HumanSignal(
    bool IsApproved,
    string ReviewerToken,
    Dictionary<string, string>? FieldOverrides = null)
{
    /// <summary>
    /// Overrides written with their JSON type: <c>["replicas"] = 3</c>, <c>["dry_run"] = false</c>,
    /// <c>["plan"] = JsonSerializer.SerializeToNode(plan)</c>. A JSON null sets the field to null. Each
    /// replaces a top-level field of the state. A key may not also be in <see cref="FieldOverrides"/>.
    /// The engine copies every value it applies, so a signal or a value can be reused.
    /// </summary>
    public IReadOnlyDictionary<string, JsonNode?>? Overrides { get; init; }

    /// <summary>An approval by <paramref name="reviewer"/>, with no overrides.</summary>
    public static HumanSignal Approve(string reviewer) => new(true, reviewer);

    /// <summary>A rejection by <paramref name="reviewer"/>, with no overrides.</summary>
    public static HumanSignal Reject(string reviewer) => new(false, reviewer);

    /// <summary>A copy of this signal with one more typed override (replacing any for <paramref name="key"/>).</summary>
    public HumanSignal WithOverride(string key, JsonNode? value)
    {
        ArgumentNullException.ThrowIfNull(key);
        var overrides = Overrides is null
            ? new Dictionary<string, JsonNode?>(StringComparer.Ordinal)
            : new Dictionary<string, JsonNode?>(Overrides, StringComparer.Ordinal);
        overrides[key] = value;
        return this with { Overrides = overrides };
    }

    /// <summary>
    /// Every override as a fresh JSON value, sorted by key: the string ones as JSON strings, the typed
    /// ones copied. A key set in both maps is a <paramref name="problem"/>, and the result is then empty.
    /// </summary>
    internal SortedDictionary<string, JsonNode?> EffectiveOverrides(out string? problem)
    {
        problem = null;
        var effective = new SortedDictionary<string, JsonNode?>(StringComparer.Ordinal);
        if (FieldOverrides is not null)
        {
            foreach (var (key, value) in FieldOverrides)
            {
                effective[key] = JsonValue.Create(value);
            }
        }

        if (Overrides is not null)
        {
            foreach (var (key, value) in Overrides)
            {
                if (FieldOverrides?.ContainsKey(key) == true)
                {
                    problem = $"override '{key}' is set as both a string and a typed value";
                    return new SortedDictionary<string, JsonNode?>(StringComparer.Ordinal);
                }

                effective[key] = value?.DeepClone();
            }
        }

        return effective;
    }
}
