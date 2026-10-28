using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;

namespace Datalium.Strata.Cloud;

/// <summary>
/// How a checkpoint compares with Cloud's copy of it (<c>design/30-Strata-Cloud-Store.md</c>,
/// "Normalisation"). Cloud stores <c>frozen_state</c> and <c>metadata</c> as JSONB and normalises
/// timestamps, so its copy can differ in bytes but not in content.
/// </summary>
public static class StrataCloudCheckpoints
{
    /// <summary>
    /// Whether <paramref name="left"/> and <paramref name="right"/> are the same checkpoint as Cloud sees it:
    /// object keys in any order, numbers by value (<c>1.0</c> equals <c>1</c>), and <c>created_at</c> /
    /// <c>resumed_at</c> as instants at whole seconds. Everything else exactly.
    /// </summary>
    public static bool AreEquivalent(Checkpoint left, Checkpoint right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return Equivalent(left.ToWireJson(), right.ToWireJson(), key: null);
    }

    private static bool Equivalent(JsonNode? left, JsonNode? right, string? key)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        switch (left)
        {
            case JsonObject leftObject when right is JsonObject rightObject:
                return leftObject.Count == rightObject.Count
                    && leftObject.All(pair =>
                        rightObject.TryGetPropertyValue(pair.Key, out var other) && Equivalent(pair.Value, other, pair.Key));
            case JsonArray leftArray when right is JsonArray rightArray:
                return leftArray.Count == rightArray.Count
                    && leftArray.Select((item, i) => Equivalent(item, rightArray[i], key: null)).All(same => same);
            case JsonValue leftValue when right is JsonValue rightValue:
                return ValuesEquivalent(leftValue, rightValue, key);
            default:
                return false;
        }
    }

    private static bool ValuesEquivalent(JsonValue left, JsonValue right, string? key)
    {
        var leftKind = left.GetValueKind();
        if (leftKind != right.GetValueKind())
        {
            return false;
        }

        switch (leftKind)
        {
            case JsonValueKind.Number:
                var leftText = left.ToJsonString();
                var rightText = right.ToJsonString();
                return decimal.TryParse(leftText, NumberStyles.Float, CultureInfo.InvariantCulture, out var leftDecimal)
                    && decimal.TryParse(rightText, NumberStyles.Float, CultureInfo.InvariantCulture, out var rightDecimal)
                        ? leftDecimal == rightDecimal
                        : double.Parse(leftText, CultureInfo.InvariantCulture) == double.Parse(rightText, CultureInfo.InvariantCulture);
            case JsonValueKind.String:
                var leftString = left.GetValue<string>();
                var rightString = right.GetValue<string>();
                return leftString == rightString
                    || (key is "created_at" or "resumed_at" && SameSecond(leftString, rightString));
            default:
                return left.ToJsonString() == right.ToJsonString();
        }
    }

    private static bool SameSecond(string left, string right) =>
        DateTimeOffset.TryParse(left, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var leftTime)
        && DateTimeOffset.TryParse(right, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var rightTime)
        && leftTime.ToUnixTimeSeconds() == rightTime.ToUnixTimeSeconds();
}
