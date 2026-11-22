// Samples for https://stratamesh.dev/guides/production/observability/

// docs:begin guides/production/observability/opentelemetry
using Datalium.Strata.Observability;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

// docs:hide
using System.Collections.Concurrent;
using System.Diagnostics;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;

namespace Datalium.Strata.DocsSnippets.Guides.Production.Observability;

public class ObservabilitySamples
{
    [Fact]
    public async Task ExportWithOpenTelemetry()
    {
        var spans = new List<Activity>();
        var exported = new List<Metric>();
        // docs:show
        using var tracing = Sdk.CreateTracerProviderBuilder()
            .AddSource(StrataTelemetry.Name)
            // docs:hide
            .AddInMemoryExporter(spans)
            // docs:show
            .AddConsoleExporter()
            .Build();
        using var metrics = Sdk.CreateMeterProviderBuilder()
            .AddMeter(StrataTelemetry.Name)
            // docs:hide
            .AddInMemoryExporter(exported)
            // docs:show
            .AddConsoleExporter()
            .Build();
        // docs:end guides/production/observability/opentelemetry

        // Run the review graph: the first attempt of Classify fails transiently.
        const string thread = "observability-order-4001";
        var result = await ReviewGraph().RunAsync(thread, Order.New(40), new InMemoryCheckpointStore());
        Assert.IsType<GraphExecutionResult<Order>.Interrupted>(result);
        tracing.ForceFlush();
        metrics.ForceFlush();

        var attempt = Assert.Single(spans, span =>
            span.DisplayName == "Classify #1" && (string?)span.GetTagItem("strata.thread_id") == thread);
        Assert.Equal(ActivityKind.Internal, attempt.Kind);
        Assert.Equal("Classify", attempt.GetTagItem("strata.node.path"));
        Assert.Equal(1, Convert.ToInt32(attempt.GetTagItem("strata.attempt")));
        Assert.Equal("transient", attempt.GetTagItem("error.type"));
        Assert.Equal(ActivityStatusCode.Error, attempt.Status);
        Assert.Equal("gateway timeout", attempt.StatusDescription);
        Assert.Contains(exported, metric => metric.Name == "strata.node.retries");
    }

    [Fact]
    public async Task Stream()
    {
        Console.Lines.Clear();

        // docs:begin guides/production/observability/stream
        var graph = new StateGraph<Answer>()
            .AddStreamingNode("Answer", async (Answer state, NodeContext ctx, CancellationToken ct) =>
            {
                var text = "";
                foreach (var token in new[] { "Checkpoints ", "make ", "runs ", "durable." })
                {
                    await ctx.Stream!.EmitTokenAsync(token, ct);
                    text += token;
                }

                return state with { Text = text };
            })
            .AddNode("Count", state => state with { Text = $"{state.Text} ({state.Text.Length} chars)" })
            .AddEdge("Answer", "Count")
            .Compile();

        var (reader, completion) = graph.RunStreaming("answer-1", new Answer(""), new InMemoryCheckpointStore());

        await foreach (var chunk in reader.ReadAllAsync())
        {
            Console.WriteLine(chunk.Type switch
            {
                StreamChunkType.Token => $"  {chunk.NodeName}: \"{chunk.Payload}\"",
                StreamChunkType.Retry or StreamChunkType.Error => $"{chunk.Type} {chunk.NodeName}: {chunk.Payload}",
                _ => $"{chunk.Type} {chunk.NodeName}",
            });
        }

        var result = await completion;
        Console.WriteLine(result is GraphExecutionResult<Answer>.Completed done ? $"Completed: {done.State.Text}" : $"{result}");
        // docs:end guides/production/observability/stream

        Assert.Equal(
            [
                "NodeStart Answer",
                "  Answer: \"Checkpoints \"",
                "  Answer: \"make \"",
                "  Answer: \"runs \"",
                "  Answer: \"durable.\"",
                "NodeEnd Answer",
                "NodeStart Count",
                "NodeEnd Count",
                "Completed: Checkpoints make runs durable. (30 chars)",
            ],
            Console.Take());

        // docs:begin guides/production/observability/plain-run
        var plain = await graph.RunAsync("answer-2", new Answer(""), new InMemoryCheckpointStore());
        if (plain is GraphExecutionResult<Answer>.Failed failed)
        {
            Console.WriteLine(failed.Message);
        }
        // docs:end guides/production/observability/plain-run

        Assert.Equal(
            ["node 'Answer' is a streaming node but this run has no StreamContext — use RunStreaming instead of RunAsync"],
            Console.Take());
    }

    [Fact]
    public async Task Events()
    {
        Console.Lines.Clear();
        var graph = ReviewGraph();
        var store = new InMemoryCheckpointStore();

        // docs:begin guides/production/observability/events
        var events = new RunEventChannel();
        var result = await graph.RunAsync("order-2001", Order.New(40), store, new RunOptions
        {
            Metadata = { ["tenant"] = "acme" },
            Observer = events,
        });

        await foreach (var e in events.Reader.ReadAllAsync())
        {
            Console.WriteLine($"#{e.Sequence} {e.GetType().Name} {e.NodePath}{(e.Attempt is { } a ? $" #{a}" : "")}");
        }
        // docs:end guides/production/observability/events

        Assert.IsType<GraphExecutionResult<Order>.Interrupted>(result);
        Assert.Equal(
            [
                "#1 RunStarted ",
                "#2 NodeStarted Classify #1",
                "#3 RetryScheduled Classify #1",
                "#4 NodeCompleted Classify #2",
                "#5 NodeStarted Approve #1",
                "#6 NodeCompleted Approve #1",
                "#7 CheckpointSaved ",
                "#8 Interrupted ",
                "#9 RunCompleted ",
            ],
            Console.Take());
    }

    // docs:begin guides/production/observability/observer
    sealed class ConsoleObserver : IRunObserver
    {
        public void OnEvent(RunEvent runEvent)
        {
            var line = runEvent switch
            {
                NodeCompleted n => $"{runEvent.NodePath} {n.Result} in {n.Attempt} attempt(s)",
                RetryScheduled r => $"{runEvent.NodePath} attempt {r.Attempt} failed: {r.Error}",
                ModelCallCompleted m => $"{m.Model}: {m.Usage.InputTokens} in, {m.Usage.OutputTokens} out",
                RunCompleted c => $"run {runEvent.RunId} {c.Outcome}",
                _ => null, // later versions add event types
            };
            if (line is not null)
            {
                Console.WriteLine($"[{runEvent.ThreadId}] {line}");
            }
        }
    }

    // docs:hide
    [Fact]
    public async Task Observer()
    {
        Console.Lines.Clear();
        var graph = ReviewGraph();
        var store = new InMemoryCheckpointStore();
        // docs:show
        await graph.RunAsync("order-2002", Order.New(40), store, new RunOptions { Observer = new ConsoleObserver() });
        // docs:end guides/production/observability/observer

        // The run can return before the observer has seen its last event.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        var lines = new List<string>();
        while (lines.Count == 0 || !lines[^1].StartsWith("[order-2002] run ", StringComparison.Ordinal))
        {
            Assert.True(DateTime.UtcNow < deadline, string.Join("\n", lines));
            await Task.Delay(5);
            lines.AddRange(Console.Take());
        }

        Assert.EndsWith(" Interrupted", lines[^1]);
        Assert.Equal(
            [
                "[order-2002] Classify attempt 1 failed: gateway timeout",
                "[order-2002] Classify Committed in 2 attempt(s)",
                "[order-2002] Approve Committed in 1 attempt(s)",
            ],
            lines[..^1]);
    }

    // Classify fails transiently once and is retried, then the run pauses at the interrupt boundary on Approve.
    private static CompiledGraph<Order> ReviewGraph()
    {
        var calls = 0;
        return new StateGraph<Order>()
            .AddFallibleNode("Classify", (Order o, NodeContext _, CancellationToken _) =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    throw NodeException.Transient("gateway timeout");
                }

                return ValueTask.FromResult(o.Then("classify"));
            })
            .AddNode("Approve", o => o.Then("approve"))
            .AddInterruptBoundary("Approve")
            .AddNode("Execute", o => o.Then("execute"))
            .AddEdge("Classify", "Approve")
            .AddEdge("Approve", "Execute")
            .SetRecoveryPolicy("Classify", RecoveryPolicy.Retry(1))
            .Compile();
    }
}

internal sealed record Answer(string Text);

internal sealed record Order(int Amount, IReadOnlyList<string> Log)
{
    public static Order New(int amount) => new(amount, []);

    public Order Then(string step) => this with { Log = [.. Log, step] };
}

/// <summary>
/// The samples print what they find. In this namespace, <c>Console.WriteLine</c> is this class: it keeps the
/// lines, so the tests can check them.
/// </summary>
internal static class Console
{
    public static ConcurrentQueue<string> Lines { get; } = new();

    public static void WriteLine(string? line) => Lines.Enqueue(line ?? "");

    public static List<string> Take()
    {
        var lines = new List<string>();
        while (Lines.TryDequeue(out var line))
        {
            lines.Add(line);
        }

        return lines;
    }
}
