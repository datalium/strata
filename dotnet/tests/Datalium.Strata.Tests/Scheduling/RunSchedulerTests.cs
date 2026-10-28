using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Datalium.Strata.Scheduling;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.Scheduling;

/// <summary>
/// The run scheduler on its own (<c>design/40-Run-Scheduler.md</c>): the shared cases in
/// <c>schema/fixtures/scheduler/cases.json</c>, which <c>rust/strata-core/tests/scheduler.rs</c> replays too, and
/// the waits that need a clock. Keep the two in step.
/// </summary>
public sealed class RunSchedulerTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    internal static JsonNode Fixture(string relativePath, [CallerFilePath] string sourceFilePath = "")
    {
        var testDir = Path.GetDirectoryName(sourceFilePath)!;
        var path = Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "..", "schema", "fixtures", relativePath));
        return JsonNode.Parse(File.ReadAllText(path))!;
    }

    private static JsonArray Cases() => Fixture(Path.Combine("scheduler", "cases.json"))["cases"]!.AsArray();

    public static TheoryData<string> CaseNames()
    {
        var names = new TheoryData<string>();
        foreach (var @case in Cases())
        {
            names.Add(@case!["name"]!.GetValue<string>());
        }

        return names;
    }

    private static RunSchedulerOptions OptionsFrom(JsonNode? json)
    {
        var defaults = new RunSchedulerOptions();
        if (json is null)
        {
            return defaults;
        }

        return new RunSchedulerOptions
        {
            MaxConcurrent = json["max_concurrent"]?.GetValue<int>() ?? defaults.MaxConcurrent,
            MaxQueued = json["max_queued"]?.GetValue<int>() ?? defaults.MaxQueued,
            Reservations = json["reservations"]?.AsArray()
                .Select(r => new SlotReservation(r!["min_priority"]!.GetValue<int>(), r["slots"]!.GetValue<int>()))
                .ToList() ?? [],
            TenantLimit = json["tenant_limit"]?.GetValue<int>(),
            TenantLimits = json["tenant_limits"]?.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<int>())
                ?? new Dictionary<string, int>(),
            TenantMaxQueued = json["tenant_max_queued"]?.GetValue<int>(),
        };
    }

    private static string Wire(NotAdmittedReason reason) => reason switch
    {
        NotAdmittedReason.QueueFull => "queue_full",
        NotAdmittedReason.Shed => "shed",
        NotAdmittedReason.QueueTimeout => "queue_timeout",
        NotAdmittedReason.Cancelled => "cancelled",
        NotAdmittedReason.Deadline => "deadline",
        _ => "closed",
    };

    /// <summary>One run of a case: its pending or decided admission, and its token.</summary>
    private sealed class Run(Task<Admission> admission, CancellationTokenSource token)
    {
        public Task<Admission> Admission { get; } = admission;

        public CancellationTokenSource Token { get; } = token;
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public async Task Shared_case(string name)
    {
        var @case = Cases().Single(c => c!["name"]!.GetValue<string>() == name)!;
        var scheduler = new RunScheduler(OptionsFrom(@case["options"]));
        var runs = new Dictionary<string, Run>();
        var active = new SortedSet<string>(StringComparer.Ordinal);
        var queued = new SortedSet<string>(StringComparer.Ordinal);
        var stepNumber = 0;
        foreach (var step in @case["steps"]!.AsArray())
        {
            stepNumber++;
            var at = $"{name}, step {stepNumber}";
            if (step!["arrive"] is { } arrive)
            {
                var id = arrive.GetValue<string>();
                var token = new CancellationTokenSource();
                var request = new AdmissionRequest
                {
                    Priority = step["priority"]?.GetValue<int>() ?? 0,
                    Tenant = step["tenant"]?.GetValue<string>(),
                };
                runs[id] = new Run(scheduler.AdmitAsync(request, token.Token).AsTask(), token);
                queued.Add(id);
            }
            else if (step["release"] is { } release)
            {
                var id = release.GetValue<string>();
                (await runs[id].Admission).Dispose();
                active.Remove(id);
            }
            else if (step["cancel"] is { } cancel)
            {
                await runs[cancel.GetValue<string>()].Token.CancelAsync();
            }
            else if (step["set_tenant_limit"] is { } tenant)
            {
                scheduler.SetTenantLimit(tenant.GetValue<string>(), step["limit"]?.GetValue<int>());
            }
            else if (step["close"] is not null)
            {
                scheduler.Close();
            }

            var expectedActive = step["active"]!.AsArray().Select(n => n!.GetValue<string>()).ToHashSet();
            var expectedQueued = step["queued"]!.AsArray().Select(n => n!.GetValue<string>()).ToHashSet();
            var expectedRefused = step["refused"]!.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<string>());

            // Every run the step decided has its decision.
            foreach (var id in queued.ToList())
            {
                if (expectedActive.Contains(id))
                {
                    var admission = await runs[id].Admission.WaitAsync(Patience);
                    Assert.True(admission.IsAdmitted, $"{at}: '{id}' should be admitted, was refused {admission.Refusal}");
                    queued.Remove(id);
                    active.Add(id);
                }
                else if (expectedRefused.TryGetValue(id, out var reason))
                {
                    var admission = await runs[id].Admission.WaitAsync(Patience);
                    Assert.False(admission.IsAdmitted, $"{at}: '{id}' should be refused {reason}, was admitted");
                    Assert.Equal(reason, Wire(admission.Refusal!.Value));
                    queued.Remove(id);
                }
            }

            Assert.Equal(expectedActive.Order(StringComparer.Ordinal), active);
            Assert.Equal(expectedQueued.Order(StringComparer.Ordinal), queued);
            Assert.All(queued, id => Assert.False(runs[id].Admission.IsCompleted, $"{at}: '{id}' should still be waiting"));
            var snapshot = scheduler.Snapshot();
            Assert.Equal((active.Count, queued.Count), (snapshot.Active, snapshot.Queued));
        }
    }

    private static RunScheduler OneSlot(TimeSpan? maxQueueTime = null) =>
        new(new RunSchedulerOptions { MaxConcurrent = 1, MaxQueueTime = maxQueueTime });

    [Fact]
    public async Task A_run_waits_at_most_max_queue_time()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var scheduler = OneSlot(TimeSpan.FromSeconds(30));
        using var holder = await scheduler.AdmitAsync(new AdmissionRequest { TimeProvider = clock });

        var waiting = scheduler.AdmitAsync(new AdmissionRequest { TimeProvider = clock }).AsTask();
        clock.Advance(TimeSpan.FromSeconds(29));
        await Task.Delay(50);
        Assert.False(waiting.IsCompleted);

        clock.Advance(TimeSpan.FromSeconds(1));
        var refused = await waiting.WaitAsync(Patience);
        Assert.Equal(NotAdmittedReason.QueueTimeout, refused.Refusal);
        Assert.Equal(TimeSpan.FromSeconds(30), refused.Waited);
        Assert.Equal(0, scheduler.Snapshot().Queued);
    }

    [Fact]
    public async Task A_deadline_ends_a_wait_and_the_earlier_of_the_two_says_why()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var scheduler = OneSlot(TimeSpan.FromMinutes(5));
        using var holder = await scheduler.AdmitAsync(new AdmissionRequest { TimeProvider = clock });

        var waiting = scheduler
            .AdmitAsync(new AdmissionRequest { TimeProvider = clock, Deadline = clock.GetUtcNow() + TimeSpan.FromSeconds(10) })
            .AsTask();
        clock.Advance(TimeSpan.FromSeconds(10));

        var refused = await waiting.WaitAsync(Patience);
        Assert.Equal(NotAdmittedReason.Deadline, refused.Refusal);
        Assert.Equal(TimeSpan.FromSeconds(10), refused.Waited);
    }

    [Fact]
    public async Task A_deadline_already_passed_refuses_a_wait_at_once_but_not_a_free_slot()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var scheduler = OneSlot();
        var late = new AdmissionRequest { TimeProvider = clock, Deadline = clock.GetUtcNow() - TimeSpan.FromSeconds(1) };

        // A free slot is taken: the run's own deadline check stops it, as for a run with no scheduler.
        using var admitted = await scheduler.AdmitAsync(late);
        Assert.True(admitted.IsAdmitted);

        var refused = await scheduler.AdmitAsync(late).AsTask().WaitAsync(Patience);
        Assert.Equal(NotAdmittedReason.Deadline, refused.Refusal);
    }

    [Fact]
    public async Task An_admission_that_races_its_timeout_keeps_its_slot()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var scheduler = OneSlot(TimeSpan.FromSeconds(10));
        var holder = await scheduler.AdmitAsync(new AdmissionRequest { TimeProvider = clock });
        var waiting = scheduler.AdmitAsync(new AdmissionRequest { TimeProvider = clock }).AsTask();

        // Admitted under the lock first; the timer that fires next finds it decided.
        holder.Dispose();
        clock.Advance(TimeSpan.FromSeconds(10));

        using var admitted = await waiting.WaitAsync(Patience);
        Assert.True(admitted.IsAdmitted);
        Assert.Equal(1, scheduler.Snapshot().Active);
    }

    [Fact]
    public async Task A_slot_given_back_twice_counts_once()
    {
        var scheduler = new RunScheduler(new RunSchedulerOptions { MaxConcurrent = 2 });
        var first = await scheduler.AdmitAsync(new AdmissionRequest());
        using var second = await scheduler.AdmitAsync(new AdmissionRequest());

        first.Dispose();
        first.Dispose();

        Assert.Equal(1, scheduler.Snapshot().Active);
        using var third = await scheduler.AdmitAsync(new AdmissionRequest());
        Assert.True(third.IsAdmitted);
        var fourth = scheduler.AdmitAsync(new AdmissionRequest()).AsTask();
        Assert.False(fourth.IsCompleted);
        scheduler.Close();
        Assert.Equal(NotAdmittedReason.Closed, (await fourth.WaitAsync(Patience)).Refusal);
    }

    [Fact]
    public async Task A_refused_admission_gives_nothing_back()
    {
        var scheduler = new RunScheduler(new RunSchedulerOptions { MaxConcurrent = 1, MaxQueued = 0 });
        var held = await scheduler.AdmitAsync(new AdmissionRequest());
        var refused = await scheduler.AdmitAsync(new AdmissionRequest());

        refused.Dispose();

        Assert.Equal(NotAdmittedReason.QueueFull, refused.Refusal);
        Assert.Equal(1, scheduler.Snapshot().Active);
        held.Dispose();
        Assert.Equal(0, scheduler.Snapshot().Active);
    }

    [Fact]
    public async Task The_snapshot_shows_each_tenant_and_its_limit()
    {
        var scheduler = new RunScheduler(new RunSchedulerOptions
        {
            Name = "investigations",
            MaxConcurrent = 4,
            TenantLimit = 2,
            TenantLimits = new Dictionary<string, int> { ["paused"] = 0 },
        });
        using var a = await scheduler.AdmitAsync(new AdmissionRequest { Tenant = "acme" });
        var waiting = scheduler.AdmitAsync(new AdmissionRequest { Tenant = "paused" }).AsTask();

        var snapshot = scheduler.Snapshot();

        Assert.Equal("investigations", snapshot.Name);
        Assert.Equal((1, 1, 4, 1024, false), (snapshot.Active, snapshot.Queued, snapshot.MaxConcurrent, snapshot.MaxQueued, snapshot.IsClosed));
        Assert.Equal(new TenantUsage(1, 0, 2), snapshot.Tenants["acme"]);
        Assert.Equal(new TenantUsage(0, 1, 0), snapshot.Tenants["paused"]);

        scheduler.Close();
        Assert.Equal(NotAdmittedReason.Closed, (await waiting.WaitAsync(Patience)).Refusal);
        Assert.True(scheduler.IsClosed);
        Assert.Equal(new TenantUsage(0, 0, 0), scheduler.Snapshot().Tenants["paused"]);
    }

    [Fact]
    public void Invalid_options_are_refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RunScheduler(new RunSchedulerOptions { MaxConcurrent = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RunScheduler(new RunSchedulerOptions { MaxQueued = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RunScheduler(new RunSchedulerOptions { MaxQueueTime = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RunScheduler(new RunSchedulerOptions { TenantLimit = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RunScheduler(new RunSchedulerOptions { TenantMaxQueued = -1 }));
        Assert.Throws<ArgumentException>(() => new RunScheduler(new RunSchedulerOptions { TenantKey = " " }));
        Assert.Throws<ArgumentException>(() => new RunScheduler(new RunSchedulerOptions { Name = "" }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RunScheduler(new RunSchedulerOptions
        {
            TenantLimits = new Dictionary<string, int> { ["acme"] = -1 },
        }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RunScheduler(new RunSchedulerOptions
        {
            Reservations = [new SlotReservation(3, 0)],
        }));

        var duplicate = Assert.Throws<ArgumentException>(() => new RunScheduler(new RunSchedulerOptions
        {
            Reservations = [new SlotReservation(3, 1), new SlotReservation(3, 1)],
        }));
        Assert.Contains("two reservations are for priority 3", duplicate.Message);

        var everything = Assert.Throws<ArgumentException>(() => new RunScheduler(new RunSchedulerOptions
        {
            MaxConcurrent = 4,
            Reservations = [new SlotReservation(3, 2), new SlotReservation(2, 2)],
        }));
        Assert.Contains("the reservations keep 4 of 4 slots", everything.Message);

        var scheduler = new RunScheduler(new RunSchedulerOptions());
        Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.SetTenantLimit("acme", -1));
        Assert.Throws<ArgumentException>(() => scheduler.SetTenantLimit(" ", 1));
    }
}
