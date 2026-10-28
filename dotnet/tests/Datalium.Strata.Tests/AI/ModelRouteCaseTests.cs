using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Nodes;
using Datalium.Strata.AI;
using Datalium.Strata.Observability;
using Datalium.Strata.Tests.Scheduling;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.Tests.AI;

/// <summary>Runs the clock-driven route cases alone, so a busy thread pool can't make the clock run ahead of them.</summary>
[CollectionDefinition(nameof(ModelRouteClock), DisableParallelization = true)]
public sealed class ModelRouteClock;

/// <summary>
/// The model route's shared cases (<c>design/46-Model-Routes.md</c>, "The exact rules"):
/// <c>schema/fixtures/ai/route-cases.json</c>, which <c>rust/strata-ai/tests/route_cases.rs</c> replays too. Keep the two
/// in step. Each case runs on a fake clock that moves from one pending timer to the next once the route has settled.
/// </summary>
[Collection(nameof(ModelRouteClock))]
public sealed class ModelRouteCaseTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private static JsonArray Cases() => RunSchedulerTests.Fixture(Path.Combine("ai", "route-cases.json"))["cases"]!.AsArray();

    private static JsonNode Case(string name) => Cases().Single(c => c!["name"]!.GetValue<string>() == name)!;

    public static TheoryData<string> CaseNames()
    {
        var names = new TheoryData<string>();
        foreach (var @case in Cases())
        {
            names.Add(@case!["name"]!.GetValue<string>());
        }

        return names;
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public async Task Each_Shared_Case_Plays_As_The_Fixture_Says(string name)
    {
        var @case = Case(name);
        var fake = new FakeTimeProvider();
        var time = new TrackingTime(fake);
        var start = fake.GetUtcNow();
        var breaker = @case["breaker"];
        var health = new ModelHealth(new ModelHealthOptions
        {
            TimeProvider = time,
            Breaker = breaker is null
                ? null
                : new ModelBreaker(breaker["failures"]!.GetValue<int>(), Ms(breaker["open_for_ms"]!.GetValue<long>())),
        });
        var accept = @case["accept"];
        ModelRouteResult? routed = null;
        var route = new ModelRoute(new ModelRouteOptions
        {
            Name = "r",
            Candidates = [.. @case["candidates"]!.AsArray().Select(c => Candidate(c!, time))],
            Accept = accept is null
                ? null
                : response => response.Text.Contains(accept["reject_containing"]!.GetValue<string>(), StringComparison.Ordinal)
                    ? accept["reason"]!.GetValue<string>()
                    : null,
            HedgeAfter = @case["hedge_after_ms"] is { } hedge ? Ms(hedge.GetValue<long>()) : null,
            Health = health,
            OnRouted = result => routed = result,
        });

        var number = 0;
        foreach (var call in @case["calls"]!.AsArray())
        {
            number++;
            var at = start + Ms(call!["at_ms"]!.GetValue<long>());
            Assert.True(at >= fake.GetUtcNow(), $"call {number} starts before the clock");
            fake.SetUtcNow(at);
            routed = null;
            var messages = new List<ChatMessage> { new(ChatRole.User, "why?") };
            var options = new ChatOptions { ModelId = "requested" };
            var asked = call["stream"]?.GetValue<bool>() == true
                ? Collect(route.GetStreamingResponseAsync(messages, options))
                : route.GetResponseAsync(messages, options);

            await Drive(asked, fake, time);
            var endedAt = (long)(fake.GetUtcNow() - start).TotalMilliseconds;
            var failure = asked.Exception?.InnerException;
            var response = failure is null ? await asked : null;
            var expect = call["expect"]!;
            var where = $"{name}, call {number}";

            if (expect["answer"] is { } answer)
            {
                Assert.True(response is not null, $"{where}: {failure}");
                Assert.Equal(answer["text"]!.GetValue<string>(), response.Text);
                Assert.Equal(answer["model"]!.GetValue<string>(), response.ModelId);
                Assert.Equal(answer["candidate"]!.GetValue<string>(), routed?.Answered);
            }
            else
            {
                var error = expect["error"]!;
                Assert.True(failure is not null, $"{where}: it answered {response?.Text}");
                var fault = ModelFaults.Classify(failure);
                Assert.Equal(error["kind"]!.GetValue<string>(), fault.IsTransient ? "transient" : "fatal");
                Assert.Equal(error["error_type"]!.GetValue<string>(), fault.ErrorType);
                Assert.Equal(error["retry_after_ms"]?.GetValue<long>(), fault.RetryAfter is { } hint ? (long)hint.TotalMilliseconds : null);
                Assert.Equal(error["message"]!.GetValue<string>(), failure.Message);
                Assert.Null(routed?.Answered);
            }

            Assert.NotNull(routed);
            if (expect["ends_at_ms"] is { } endsAt)
            {
                Assert.Equal(endsAt.GetValue<long>(), endedAt);
            }

            Assert.Equal(
                expect["attempts"]!.AsArray().Select(a => $"{a!["candidate"]} {a["outcome"]}"),
                routed.Attempts.Select(a => $"{a.Candidate} {Snake(a.Outcome.ToString())}"));
            Assert.Equal(expect["hedges"]!.GetValue<int>(), routed.Hedges);
            Assert.Equal("r", routed.Route);

            if (expect["health"] is JsonObject expected)
            {
                var snapshot = health.Snapshot();
                foreach (var (key, want) in expected)
                {
                    var entry = snapshot.SingleOrDefault(e => e.Key == key);
                    Assert.True(entry is not null, $"{where}: no health for '{key}'");
                    var got = new JsonObject { ["state"] = Snake(entry.State.ToString()), ["failures"] = entry.Failures };
                    if (entry.Until is { } until)
                    {
                        got["until_ms"] = (long)(until - start).TotalMilliseconds;
                    }

                    Assert.True(JsonNode.DeepEquals(want, got), $"{where}: health '{key}' is {got.ToJsonString()}, not {want!.ToJsonString()}");
                }
            }
        }
    }

    [Fact]
    public void Every_Case_Is_Played()
    {
        Assert.Equal(27, Cases().Count);
    }

    private static TimeSpan Ms(long ms) => TimeSpan.FromMilliseconds(ms);

    private static ModelCandidate Candidate(JsonNode spec, TimeProvider time)
    {
        var name = spec["name"]!.GetValue<string>();
        var ineligible = spec["ineligible"]?.GetValue<string>();
        return new ModelCandidate(
            name,
            new CaseClient(spec["provider"]?.GetValue<string>() ?? $"p-{name}", spec["script"]!.AsArray(), time),
            spec["model"]?.GetValue<string>())
        {
            Eligible = ineligible is null ? null : (_, _, _) => ineligible,
        };
    }

    /// <summary>
    /// Moves the clock from one pending timer to the next, each time the route has settled (no timer made or dropped
    /// for a while), until <paramref name="asked"/> ends.
    /// </summary>
    private static async Task Drive(Task asked, FakeTimeProvider fake, TrackingTime time)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!asked.IsCompleted)
        {
            Assert.True(DateTime.UtcNow < deadline, "the call never ended");
            var version = time.Version;
            var quiet = 0;
            while (!asked.IsCompleted && quiet < 4)
            {
                await Task.Delay(5);
                quiet = time.Version == version ? quiet + 1 : 0;
                version = time.Version;
            }

            if (asked.IsCompleted)
            {
                break;
            }

            var next = time.NextDue();
            Assert.True(next is not null, "the call is waiting for nothing the clock can bring");
            fake.SetUtcNow(next.Value > fake.GetUtcNow() ? next.Value : fake.GetUtcNow());
        }

        await asked.ContinueWith(_ => { }, TaskScheduler.Default).WaitAsync(Patience);
    }

    private static async Task<ChatResponse> Collect(IAsyncEnumerable<ChatResponseUpdate> stream)
    {
        var updates = new List<ChatResponseUpdate>();
        await foreach (var update in stream)
        {
            updates.Add(update);
        }

        return updates.ToChatResponse();
    }

    /// <summary><c>CoolingDown</c> as the fixture writes it: <c>cooling_down</c>.</summary>
    private static string Snake(string pascal)
    {
        var text = new StringBuilder();
        foreach (var c in pascal)
        {
            if (char.IsUpper(c) && text.Length > 0)
            {
                text.Append('_');
            }

            text.Append(char.ToLowerInvariant(c));
        }

        return text.ToString();
    }

    /// <summary>
    /// A candidate's client, answering from the case's script (one step per time it is asked) on the fake clock, with
    /// the model it was asked for and usage 10 in, 5 out. Its provider name is the candidate's health key.
    /// </summary>
    private sealed class CaseClient(string provider, JsonArray script, TimeProvider time) : IChatClient
    {
        private int _asked;

        public async Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
        {
            var step = Next();
            await Delay(step, cancellationToken);
            if (step["fail"] is { } fail)
            {
                throw Failure(fail);
            }

            var text = step["answer"]?.GetValue<string>() ?? string.Concat(step["stream"]!.AsArray().Select(t => t!.GetValue<string>()));
            return new ChatResponse(new ChatMessage(ChatRole.Assistant, text))
            {
                ModelId = options?.ModelId,
                FinishReason = new ChatFinishReason(step["finish_reason"]?.GetValue<string>() ?? "stop"),
                Usage = new UsageDetails { InputTokenCount = 10, OutputTokenCount = 5 },
            };
        }

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var step = Next();
            await Delay(step, cancellationToken);
            if (step["fail"] is { } fail)
            {
                throw Failure(fail);
            }

            IEnumerable<string> texts = step["stream"] is JsonArray stream
                ? [.. stream.Select(t => t!.GetValue<string>())]
                : [step["answer"]!.GetValue<string>()];
            foreach (var text in texts)
            {
                yield return new ChatResponseUpdate(ChatRole.Assistant, text) { ModelId = options?.ModelId };
            }

            if (step["break"] is { } broken)
            {
                throw Failure(broken);
            }

            yield return new ChatResponseUpdate(ChatRole.Assistant, [new UsageContent(new UsageDetails { InputTokenCount = 10, OutputTokenCount = 5 })])
            {
                ModelId = options?.ModelId,
                FinishReason = new ChatFinishReason(step["finish_reason"]?.GetValue<string>() ?? "stop"),
            };
        }

        public object? GetService(Type serviceType, object? serviceKey = null) =>
            serviceType == typeof(ChatClientMetadata) ? new ChatClientMetadata(provider) : null;

        public void Dispose()
        {
        }

        private JsonNode Next()
        {
            var asked = Interlocked.Increment(ref _asked) - 1;
            return asked < script.Count ? script[asked]! : throw new InvalidOperationException($"{provider}'s script has no step {asked + 1}");
        }

        private Task Delay(JsonNode step, CancellationToken cancellationToken) =>
            step["delay_ms"]?.GetValue<long>() is > 0 and var ms ? Task.Delay(Ms(ms), time, cancellationToken) : Task.CompletedTask;

        private static ModelCallException Failure(JsonNode fail)
        {
            var status = fail["status"]?.GetValue<int>();
            return new ModelCallException(
                fail["message"]!.GetValue<string>(),
                status is not { } code || ModelFaults.IsTransientStatus(code),
                status,
                fail["retry_after_ms"]?.GetValue<long>() is { } hint ? Ms(hint) : null);
        }
    }

    /// <summary>
    /// The fake clock, with every timer made through it tracked: which are pending and when each is due, and a version
    /// that changes whenever one is made, changed, fired or dropped.
    /// </summary>
    private sealed class TrackingTime(FakeTimeProvider fake) : TimeProvider
    {
        private readonly Lock _gate = new();
        private readonly Dictionary<Tracked, DateTimeOffset> _pending = [];
        private int _version;

        public int Version => Volatile.Read(ref _version);

        public override DateTimeOffset GetUtcNow() => fake.GetUtcNow();

        public override long GetTimestamp() => fake.GetTimestamp();

        public override long TimestampFrequency => fake.TimestampFrequency;

        public override TimeZoneInfo LocalTimeZone => fake.LocalTimeZone;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var tracked = new Tracked(this);
            Set(tracked, dueTime);
            tracked.Inner = fake.CreateTimer(
                s =>
                {
                    Drop(tracked);
                    callback(s);
                },
                state,
                dueTime,
                period);
            return tracked;
        }

        /// <summary>When the first pending timer is due.</summary>
        public DateTimeOffset? NextDue()
        {
            lock (_gate)
            {
                return _pending.Count == 0 ? null : _pending.Values.Min();
            }
        }

        private void Set(Tracked timer, TimeSpan dueTime)
        {
            lock (_gate)
            {
                if (dueTime == Timeout.InfiniteTimeSpan)
                {
                    _pending.Remove(timer);
                }
                else
                {
                    _pending[timer] = fake.GetUtcNow() + dueTime;
                }
            }

            Interlocked.Increment(ref _version);
        }

        private void Drop(Tracked timer)
        {
            lock (_gate)
            {
                _pending.Remove(timer);
            }

            Interlocked.Increment(ref _version);
        }

        private sealed class Tracked(TrackingTime owner) : ITimer
        {
            public ITimer Inner { get; set; } = null!;

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                owner.Set(this, dueTime);
                return Inner.Change(dueTime, period);
            }

            public void Dispose()
            {
                owner.Drop(this);
                Inner.Dispose();
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
}
