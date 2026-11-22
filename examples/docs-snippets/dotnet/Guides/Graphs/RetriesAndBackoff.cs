// Samples for https://stratamesh.dev/guides/graphs/retries-and-backoff/

using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Microsoft.Extensions.Time.Testing;

namespace Datalium.Strata.DocsSnippets.Guides.Graphs.RetriesAndBackoff;

public class RetriesAndBackoffSamples
{
    private const string NewCard = "tok_visa_4242";
    private const string ExpiredCard = "tok_expired_0119";

    private static OrderState NewOrder(string orderId, string cardToken = NewCard) =>
        new(orderId, cardToken, Status: "new", ReceiptId: null, Confirmed: false, ReviewNote: null);

    // ChargePayment -> SendConfirmation, and QueueForManualReview when the policy falls back to it: the policy is its
    // only way in.
    private static StateGraph<OrderState> OrderNodes(PaymentGateway gateway, bool withFallback)
    {
        var graph = new StateGraph<OrderState>()
            // docs:begin guides/graphs/retries-and-backoff/fallible-node
            .AddFallibleNode("ChargePayment", async (order, ctx, ct) =>
            {
                try
                {
                    var receipt = await gateway.ChargeAsync(order, ctx.IdempotencyKey, ct);
                    return order with { ReceiptId = receipt, Status = "charged" };
                }
                catch (GatewayException ex) when (ex.StatusCode is 429 or 503)
                {
                    // Worth retrying. Pass the provider's Retry-After on, if it sent one.
                    throw NodeException.Transient($"gateway unavailable ({ex.StatusCode})", ex.RetryAfter, ex);
                }
                catch (GatewayException ex)
                {
                    // Retrying won't help.
                    throw NodeException.Fatal(ex.Message, ex);
                }
            })
            // docs:end guides/graphs/retries-and-backoff/fallible-node
            .AddNode("SendConfirmation", order => order with { Confirmed = true })
            // docs:begin guides/graphs/retries-and-backoff/timeout
            .SetTimeout("ChargePayment", TimeSpan.FromSeconds(1))
            // docs:end guides/graphs/retries-and-backoff/timeout
            .AddEdge("ChargePayment", "SendConfirmation");

        return withFallback
            ? graph
                // docs:begin guides/graphs/retries-and-backoff/fallback
                .AddNode("QueueForManualReview", (order, ctx, _) =>
                {
                    var failure = ctx.LastFailure!;
                    var note = $"{failure.Node} failed ({failure.Kind}) after {failure.Attempts} attempt(s): {failure.Message}";
                    if (failure.RetryAfter is { } wait)
                    {
                        note += $"; asked to wait {wait.TotalSeconds:0} s";
                    }

                    if (failure.TimedOut)
                    {
                        note += "; timed out";
                    }

                    return ValueTask.FromResult(order with { Status = "manual_review", ReviewNote = note });
                })
            // docs:end guides/graphs/retries-and-backoff/fallback
            : graph;
    }

    private static CompiledGraph<OrderState> BuildOrderGraph(PaymentGateway gateway, bool withFallback = true)
    {
        // docs:begin guides/graphs/retries-and-backoff/policy
        var policy = RecoveryPolicy.Retry(2)
            .WithBackoff(TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(2), BackoffJitter.None)
            .WithFallback("QueueForManualReview");

        // docs:hide ...
        if (!withFallback)
        {
            policy = policy with { Fallback = null };
        }

        return OrderNodes(gateway, withFallback)
        // docs:show
            .SetRecoveryPolicy("ChargePayment", policy)
            // docs:end guides/graphs/retries-and-backoff/policy
            .Compile();
    }

    [Fact]
    public async Task WatchTheRetries()
    {
        var gateway = new PaymentGateway(Outage.TwoUnavailable);
        var graph = BuildOrderGraph(gateway);
        var store = new InMemoryCheckpointStore();
        var order = NewOrder("ord-1001");
        var retries = new List<RetryScheduled>();

        // docs:begin guides/graphs/retries-and-backoff/watch
        var events = new RunEventChannel();
        var result = await graph.RunAsync(order.OrderId, order, store, new RunOptions { Observer = events });

        await foreach (var runEvent in events.Reader.ReadAllAsync())
        {
            if (runEvent is RetryScheduled retry)
            {
                Console.WriteLine($"  retry scheduled after attempt {runEvent.Attempt}: wait {retry.Delay.TotalMilliseconds:0} ms ({retry.Error})");
                // docs:hide
                retries.Add(retry);
                // docs:show
            }
        }
        // docs:end guides/graphs/retries-and-backoff/watch

        // Two 503s, then success, on the third attempt.
        Assert.Equal([1, 2], retries.Select(retry => retry.Attempt));
        Assert.Equal([TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(400)], retries.Select(retry => retry.Delay));
        Assert.All(retries, retry => Assert.Equal("gateway unavailable (503)", retry.Error));
        var completed = Assert.IsType<GraphExecutionResult<OrderState>.Completed>(result).State;
        Assert.Equal(
            "OrderState { OrderId = ord-1001, CardToken = tok_visa_4242, Status = charged, ReceiptId = RCPT-ord-1001-3, Confirmed = True, ReviewNote =  }",
            completed.ToString());
        // Every retry passes the service the same idempotency key.
        Assert.Equal(3, gateway.IdempotencyKeys.Count);
        Assert.Single(gateway.IdempotencyKeys.Distinct());
    }

    [Fact]
    public async Task RateLimitedThenFallBack()
    {
        var (result, retries) = await RunOnFakeClock(BuildOrderGraph(new PaymentGateway(Outage.RateLimited)), NewOrder("ord-1002"));

        // The 30-second hint is capped at the backoff's 2-second maximum.
        Assert.Equal([TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2)], retries.Select(retry => retry.Delay));
        Assert.All(retries, retry => Assert.Equal("gateway unavailable (429)", retry.Error));
        var completed = Assert.IsType<GraphExecutionResult<OrderState>.Completed>(result).State;
        Assert.Equal(
            "OrderState { OrderId = ord-1002, CardToken = tok_visa_4242, Status = manual_review, ReceiptId = , Confirmed = False, ReviewNote = ChargePayment failed (Transient) after 3 attempt(s): gateway unavailable (429); asked to wait 30 s }",
            completed.ToString());
    }

    [Fact]
    public async Task HungAttemptsTimeOut()
    {
        var gateway = new PaymentGateway(Outage.Hangs);
        var (result, retries) = await RunOnFakeClock(BuildOrderGraph(gateway), NewOrder("ord-1003"));

        Assert.Equal(3, gateway.Calls);
        Assert.Equal([TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(400)], retries.Select(retry => retry.Delay));
        Assert.All(retries, retry => Assert.Equal("node 'ChargePayment' timed out after 1000 ms", retry.Error));
        var completed = Assert.IsType<GraphExecutionResult<OrderState>.Completed>(result).State;
        Assert.Equal(
            "OrderState { OrderId = ord-1003, CardToken = tok_visa_4242, Status = manual_review, ReceiptId = , Confirmed = False, ReviewNote = ChargePayment failed (Transient) after 3 attempt(s): node 'ChargePayment' timed out after 1000 ms; timed out }",
            completed.ToString());
    }

    [Fact]
    public async Task ResumeAFailedThread()
    {
        // A declined card, with no fallback: one attempt, since the failure is fatal.
        var store2 = new InMemoryCheckpointStore();
        var declined = await BuildOrderGraph(new PaymentGateway(Outage.None), withFallback: false)
            .RunAsync("order-1003", NewOrder("ord-1004", ExpiredCard), store2);
        var failed = Assert.IsType<GraphExecutionResult<OrderState>.NodeFailed>(declined);
        Assert.Equal(("ChargePayment", 1, "card declined: expired"), (failed.Node, failed.Attempts, failed.Message));

        var checkpoint = store2.Load("order-1003");
        Assert.NotNull(checkpoint);
        Assert.Equal(CheckpointStatus.Failed, checkpoint.Status);
        Assert.Equal("ChargePayment", checkpoint.NextNode);
        Assert.Equal("1", checkpoint.Metadata!["attempts"]);
        Assert.Equal("card declined: expired", checkpoint.Metadata["error"]);
        Assert.Equal("fatal", checkpoint.Metadata["error_kind"]);

        var resumeGateway = new PaymentGateway(Outage.None);
        var resumeGraph = BuildOrderGraph(resumeGateway, withFallback: false);

        // docs:begin guides/graphs/retries-and-backoff/resume
        var fix = HumanSignal.Approve("support@shop.example").WithOverride("CardToken", NewCard);
        var resumed = ExpectCompleted(await resumeGraph.ResumeAsync("order-1003", fix, store2));
        // docs:end guides/graphs/retries-and-backoff/resume

        Assert.Equal(1, resumeGateway.Calls);
        Assert.Equal(("charged", true, NewCard), (resumed.Status, resumed.Confirmed, resumed.CardToken));
    }

    private static OrderState ExpectCompleted(GraphExecutionResult<OrderState> result) =>
        Assert.IsType<GraphExecutionResult<OrderState>.Completed>(result).State;

    // Runs the graph on a fake clock, moved forward until the run ends, so the waits take no time.
    private static async Task<(GraphExecutionResult<OrderState> Result, List<RetryScheduled> Retries)> RunOnFakeClock(
        CompiledGraph<OrderState> graph, OrderState order)
    {
        var clock = new FakeTimeProvider();
        var events = new RunEventChannel();
        var run = graph.RunAsync(
            order.OrderId, order, new InMemoryCheckpointStore(), new RunOptions { TimeProvider = clock, Observer = events });
        while (!run.IsCompleted)
        {
            clock.Advance(TimeSpan.FromMilliseconds(100));
            await Task.Delay(1);
        }

        var result = await run;
        var retries = new List<RetryScheduled>();
        await foreach (var runEvent in events.Reader.ReadAllAsync())
        {
            if (runEvent is RetryScheduled retry)
            {
                retries.Add(retry);
            }
        }

        return (result, retries);
    }
}

// docs:begin guides/graphs/retries-and-backoff/state
sealed record OrderState(
    string OrderId, string CardToken, string Status, string? ReceiptId, bool Confirmed, string? ReviewNote);
// docs:end guides/graphs/retries-and-backoff/state

/// <summary>How the stand-in payment gateway misbehaves. Whatever the mode, it declines an expired card.</summary>
internal enum Outage
{
    None,

    /// <summary>A 503 for the first two calls, then it works.</summary>
    TwoUnavailable,

    /// <summary>Every call is a 429 that asks the caller to wait 30 seconds.</summary>
    RateLimited,

    /// <summary>Every call hangs until it is cancelled.</summary>
    Hangs,
}

/// <summary>The stand-in for your own client's error type.</summary>
internal sealed class GatewayException(int statusCode, string message, TimeSpan? retryAfter = null) : Exception(message)
{
    public int StatusCode { get; } = statusCode;

    public TimeSpan? RetryAfter { get; } = retryAfter;
}

internal sealed class PaymentGateway(Outage outage)
{
    private int _calls;

    public int Calls => Volatile.Read(ref _calls);

    public List<string> IdempotencyKeys { get; } = [];

    public async Task<string> ChargeAsync(OrderState order, string idempotencyKey, CancellationToken ct)
    {
        var call = Interlocked.Increment(ref _calls);
        lock (IdempotencyKeys)
        {
            IdempotencyKeys.Add(idempotencyKey);
        }

        switch (outage)
        {
            case Outage.TwoUnavailable when call <= 2:
                throw new GatewayException(503, "service unavailable");
            case Outage.RateLimited:
                throw new GatewayException(429, "too many requests", TimeSpan.FromSeconds(30));
            case Outage.Hangs:
                await Task.Delay(Timeout.Infinite, ct);
                break;
        }

        if (order.CardToken.StartsWith("tok_expired", StringComparison.Ordinal))
        {
            throw new GatewayException(402, "card declined: expired");
        }

        return $"RCPT-{order.OrderId}-{call}";
    }
}
