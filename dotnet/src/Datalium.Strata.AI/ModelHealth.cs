using Datalium.Strata.Observability;

namespace Datalium.Strata.AI;

/// <summary>
/// A health key's breaker (<c>design/46-Model-Routes.md</c>, "Health"): <paramref name="Failures"/> transient failures
/// in a row open it for <paramref name="OpenFor"/>, after which one call is let through to test the key.
/// </summary>
public sealed record ModelBreaker(int Failures, TimeSpan OpenFor);

/// <summary>A <see cref="ModelHealth"/>'s options. Mirrors the Rust <c>ModelHealthOptions</c>.</summary>
public sealed record ModelHealthOptions
{
    /// <summary>The breaker every key has; <see langword="null"/> (the default) for none, so only cooldowns skip a key.</summary>
    public ModelBreaker? Breaker { get; init; }

    /// <summary>
    /// The clock for cooldowns, the breaker and the hedging of the routes given this health; <see cref="TimeProvider.System"/>
    /// when not given. Tests use <c>FakeTimeProvider</c>.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }
}

/// <summary>A health key's state, as <see cref="ModelHealth.Snapshot"/> reports it.</summary>
public enum ModelHealthState
{
    /// <summary>Its candidates are asked.</summary>
    Available,

    /// <summary>A provider's <c>Retry-After</c> is in force until <see cref="ModelHealthEntry.Until"/>.</summary>
    CoolingDown,

    /// <summary>Its breaker is open until <see cref="ModelHealthEntry.Until"/>.</summary>
    Open,

    /// <summary>Its breaker's open time has passed: the next call is let through to test it, one at a time.</summary>
    HalfOpen,
}

/// <summary>One health key in a <see cref="ModelHealth.Snapshot"/>.</summary>
/// <param name="Key">The key: a provider's name by default (<see cref="ModelCandidate.HealthKey"/>).</param>
/// <param name="Failures">Transient failures in a row: any success sets it to 0.</param>
/// <param name="Until">When the cooldown or the open breaker ends; <see langword="null"/> in the other states.</param>
public sealed record ModelHealthEntry(string Key, ModelHealthState State, int Failures, DateTimeOffset? Until);

/// <summary>
/// The health of model providers (<c>design/46-Model-Routes.md</c>, "Health"): per health key, a cooldown while a
/// provider's <c>Retry-After</c> is in force, and optionally a breaker. Thread-safe, in memory, shared by every route
/// given the same instance. Mirrors the Rust <c>ModelHealth</c>.
/// </summary>
/// <remarks>
/// <para>
/// Only transient faults count against a key (a failure before a stream's first update and a broken stream among them);
/// a refusal, a rejected answer, a lost race and a cancellation leave it alone, and any answer the route accepts from
/// the key sets its failures in a row back to 0 and closes its breaker. A cooldown lasts at most
/// <see cref="MaxCooldown"/>, whatever the provider asked for.
/// </para>
/// <para>
/// A key's state is decided when a candidate's turn comes, in this precedence: its breaker open; half-open (one call let
/// through, the others see it open); cooling down; available.
/// </para>
/// </remarks>
public sealed class ModelHealth
{
    /// <summary>
    /// The longest cooldown a provider's <c>Retry-After</c> sets (24 hours), as in the Rust engine: a hint beyond it is a
    /// broken header or a provider gone for good, and an hour-scale wait is better retried than trusted.
    /// </summary>
    public static readonly TimeSpan MaxCooldown = TimeSpan.FromHours(24);

    private readonly Lock _gate = new();
    private readonly Dictionary<string, KeyHealth> _keys = new(StringComparer.Ordinal);

    public ModelHealth(ModelHealthOptions? options = null)
    {
        Options = options ?? new ModelHealthOptions();
        if (Options.Breaker is { } breaker)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(breaker.Failures, 1, nameof(options));
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(breaker.OpenFor, TimeSpan.Zero, nameof(options));
        }

        TimeProvider = Options.TimeProvider ?? TimeProvider.System;
    }

    /// <summary>The options it was made with.</summary>
    public ModelHealthOptions Options { get; }

    /// <summary>Its clock.</summary>
    public TimeProvider TimeProvider { get; }

    /// <summary>Every key a route has asked about, by key, for a health check or a panel.</summary>
    public IReadOnlyList<ModelHealthEntry> Snapshot()
    {
        var now = TimeProvider.GetUtcNow();
        lock (_gate)
        {
            return [.. _keys.OrderBy(k => k.Key, StringComparer.Ordinal).Select(k =>
            {
                var h = k.Value;
                if (h.OpenUntil is { } open)
                {
                    return now < open
                        ? new ModelHealthEntry(k.Key, ModelHealthState.Open, h.Failures, open)
                        : new ModelHealthEntry(k.Key, ModelHealthState.HalfOpen, h.Failures, null);
                }

                return h.CoolingUntil is { } cooling && now < cooling
                    ? new ModelHealthEntry(k.Key, ModelHealthState.CoolingDown, h.Failures, cooling)
                    : new ModelHealthEntry(k.Key, ModelHealthState.Available, h.Failures, null);
            })];
        }
    }

    /// <summary>
    /// Whether a candidate under <paramref name="key"/> may be asked now: <see langword="null"/> and whether it is the
    /// half-open test, or the reason it may not and until when.
    /// </summary>
    internal Admission Admit(string key)
    {
        var now = TimeProvider.GetUtcNow();
        var halfOpen = false;
        Admission admission;
        lock (_gate)
        {
            var h = Key(key);
            if (h.OpenUntil is { } open)
            {
                if (now < open || h.Testing)
                {
                    admission = new Admission(ModelRouteOutcome.BreakerOpen, open, false);
                }
                else
                {
                    h.Testing = true;
                    halfOpen = true;
                    admission = new Admission(null, default, true);
                }
            }
            else if (h.CoolingUntil is { } cooling && now < cooling)
            {
                admission = new Admission(ModelRouteOutcome.CoolingDown, cooling, false);
            }
            else
            {
                admission = new Admission(null, default, false);
            }
        }

        if (halfOpen)
        {
            ModelRouteScope.BreakerTransition(key, "half_open");
        }

        return admission;
    }

    /// <summary>An answer the route accepted: the failures in a row go back to 0 and the breaker closes.</summary>
    internal void Succeeded(string key, bool test)
    {
        bool closed;
        lock (_gate)
        {
            var h = Key(key);
            h.Failures = 0;
            closed = h.OpenUntil is not null;
            h.OpenUntil = null;
            if (test || closed)
            {
                h.Testing = false;
            }
        }

        if (closed)
        {
            ModelRouteScope.BreakerTransition(key, "closed");
        }
    }

    /// <summary>
    /// A transient fault: one more failure in a row, a cooldown for <paramref name="retryAfter"/> (at most
    /// <see cref="MaxCooldown"/>) when the provider asked for one, kept if a cooldown already in force ends later, and
    /// the breaker opened when the failures reach its count or the half-open test failed. A breaker already open isn't
    /// pushed back.
    /// </summary>
    internal void Failed(string key, TimeSpan? retryAfter, bool test)
    {
        var now = TimeProvider.GetUtcNow();
        var opened = false;
        lock (_gate)
        {
            var h = Key(key);
            h.Failures++;
            if (retryAfter is { } hint && hint > TimeSpan.Zero)
            {
                var until = now + (hint < MaxCooldown ? hint : MaxCooldown);
                if (h.CoolingUntil is null || h.CoolingUntil < until)
                {
                    h.CoolingUntil = until;
                }
            }

            if (Options.Breaker is { } breaker)
            {
                if (test)
                {
                    h.Testing = false;
                    h.OpenUntil = now + breaker.OpenFor;
                    opened = true;
                }
                else if (h.OpenUntil is null && h.Failures >= breaker.Failures)
                {
                    h.OpenUntil = now + breaker.OpenFor;
                    opened = true;
                }
            }
        }

        if (opened)
        {
            ModelRouteScope.BreakerTransition(key, "open");
        }
    }

    /// <summary>An attempt that says nothing of the key's health ended: a half-open test lets the next call test it.</summary>
    internal void Released(string key, bool test)
    {
        if (!test)
        {
            return;
        }

        lock (_gate)
        {
            Key(key).Testing = false;
        }
    }

    /// <summary>How long until a candidate under <paramref name="key"/> may be asked again; zero when it may be now.</summary>
    internal TimeSpan WaitFor(string key)
    {
        var now = TimeProvider.GetUtcNow();
        lock (_gate)
        {
            if (!_keys.TryGetValue(key, out var h))
            {
                return TimeSpan.Zero;
            }

            if (h.OpenUntil is { } open)
            {
                return now < open ? open - now : TimeSpan.Zero;
            }

            return h.CoolingUntil is { } cooling && now < cooling ? cooling - now : TimeSpan.Zero;
        }
    }

    private KeyHealth Key(string key)
    {
        if (!_keys.TryGetValue(key, out var h))
        {
            h = new KeyHealth();
            _keys[key] = h;
        }

        return h;
    }

    /// <summary>What <see cref="Admit"/> decided: <paramref name="Refusal"/> is <see langword="null"/> when the candidate may be asked.</summary>
    internal readonly record struct Admission(ModelRouteOutcome? Refusal, DateTimeOffset Until, bool Test);

    private sealed class KeyHealth
    {
        public int Failures;

        public DateTimeOffset? CoolingUntil;

        /// <summary>When the breaker's open time ends; <see langword="null"/> while it is closed. Past it, it is half-open.</summary>
        public DateTimeOffset? OpenUntil;

        /// <summary>A half-open test is in flight.</summary>
        public bool Testing;
    }
}
