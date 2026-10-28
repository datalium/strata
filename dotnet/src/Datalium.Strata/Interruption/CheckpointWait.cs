using System.Text.Json.Nodes;

namespace Datalium.Strata.Interruption;

/// <summary>
/// What a waiting thread waits for (<c>design/29-Event-Waits-And-Timers.md</c>). Mirrors the Rust engine's
/// <c>WaitKind</c>.
/// </summary>
public enum WaitKind
{
    /// <summary>A human, at an interrupt boundary with a timeout: <c>ResumeAsync</c> answers it.</summary>
    Human,

    /// <summary>An external event, at a wait node: <c>SignalAsync</c> with its event type answers it.</summary>
    Event,

    /// <summary>A time, at a timer: <c>WakeDueAsync</c> wakes it once it is due.</summary>
    Timer,
}

/// <summary>
/// Why a checkpoint level waits: its <c>wait</c> object (<c>design/29-Event-Waits-And-Timers.md</c>).
/// <paramref name="EventType"/> is set exactly for <see cref="WaitKind.Event"/>. <paramref name="WakeAt"/> is
/// when a timer fires or a wait times out, as an RFC 3339 string at whole seconds; <see langword="null"/>
/// for an event wait with no timeout, which is never due. Mirrors the Rust engine's <c>CheckpointWait</c>.
/// </summary>
public sealed record CheckpointWait(WaitKind Kind, string? EventType, string? WakeAt)
{
    /// <summary>This wait as the checkpoint's <c>wait</c> JSON: every key always written.</summary>
    public JsonObject ToWireJson() => new()
    {
        ["kind"] = KindName(Kind),
        ["event_type"] = EventType,
        ["wake_at"] = WakeAt,
    };

    /// <summary>The inverse of <see cref="ToWireJson"/>.</summary>
    public static CheckpointWait FromWireJson(JsonNode node)
    {
        var json = node.AsObject();
        return new CheckpointWait(
            ParseKind(json["kind"]!.GetValue<string>()),
            json["event_type"]?.GetValue<string>(),
            json["wake_at"]?.GetValue<string>());
    }

    /// <summary>The wire name of <paramref name="kind"/>: <c>human</c>, <c>event</c> or <c>timer</c>.</summary>
    public static string KindName(WaitKind kind) => kind switch
    {
        WaitKind.Human => "human",
        WaitKind.Event => "event",
        WaitKind.Timer => "timer",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown wait kind"),
    };

    /// <summary>The inverse of <see cref="KindName"/>.</summary>
    public static WaitKind ParseKind(string name) => name switch
    {
        "human" => WaitKind.Human,
        "event" => WaitKind.Event,
        "timer" => WaitKind.Timer,
        _ => throw new InvalidOperationException($"unknown wait kind '{name}' in the checkpoint log"),
    };

    /// <summary>What it waits for, for messages: <c>event 'x'</c>, <c>a timer</c>, <c>a human</c>.</summary>
    internal string Describe() => Kind switch
    {
        WaitKind.Event => $"event '{EventType}'",
        WaitKind.Timer => "a timer",
        _ => "a human",
    };
}

/// <summary>
/// An external event for a thread waiting for it (<c>design/29-Event-Waits-And-Timers.md</c>), passed to
/// <c>CompiledGraph.SignalAsync</c>. Its <see cref="Overrides"/> replace top-level fields of the waiting
/// level's state, as a <see cref="HumanSignal"/>'s typed overrides do, so the payload is in the state
/// every later node and checkpoint carries. It records no <see cref="HumanDecision"/>. Mirrors the Rust
/// engine's <c>EventSignal</c>.
/// </summary>
/// <param name="EventType">The event, e.g. <c>deployment.finished</c>: it must be the one the thread waits for.</param>
public sealed record EventSignal(string EventType)
{
    /// <summary>
    /// The payload, by top-level field of the state, with its JSON type. A JSON null sets the field to
    /// null. The engine copies every value it applies.
    /// </summary>
    public IReadOnlyDictionary<string, JsonNode?>? Overrides { get; init; }

    /// <summary>A copy of this signal with one more override (replacing any for <paramref name="key"/>).</summary>
    public EventSignal WithOverride(string key, JsonNode? value)
    {
        ArgumentNullException.ThrowIfNull(key);
        var overrides = Overrides is null
            ? new Dictionary<string, JsonNode?>(StringComparer.Ordinal)
            : new Dictionary<string, JsonNode?>(Overrides, StringComparer.Ordinal);
        overrides[key] = value;
        return this with { Overrides = overrides };
    }
}

/// <summary>
/// What woke a waiting thread (<c>design/29-Event-Waits-And-Timers.md</c>): the <c>Wake</c> of a
/// <c>Resumed</c> event, and the reason <c>WakeDueAsync</c> reports. Mirrors the Rust engine's <c>WakeReason</c>.
/// </summary>
public enum WakeReason
{
    /// <summary>A human resume (<c>ResumeAsync</c>), of a waiting, failed or cancelled thread.</summary>
    Human,

    /// <summary>A signal of the event the thread waited for (<c>SignalAsync</c>).</summary>
    Event,

    /// <summary>Its timer was due (<c>WakeDueAsync</c>).</summary>
    Timer,

    /// <summary>Its wait timed out (<c>WakeDueAsync</c>): it continued at the wait's timeout route, or was ended.</summary>
    Timeout,
}
