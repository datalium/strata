using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Observability;
using Datalium.Strata.Storage;
using Datalium.Strata.Streaming;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.Tests.AI;

/// <summary>
/// The embedding call and the memory helper (<c>design/45-Long-Term-Memory.md</c>, PR 3): observed as a model call
/// with operation <c>embeddings</c> (input tokens only), never streamed, failed as the fault table says, and the
/// helper's labels and refusals beyond the shared cases (<see cref="MemoryCaseTests"/>). Mirrors
/// <c>rust/strata-ai/tests/embeddings.rs</c> and <c>memory.rs</c>.
/// </summary>
public class EmbeddingTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed record Job(int Vectors = 0);

    private static async Task<List<RunEvent>> Drain(RunEventChannel channel)
    {
        var events = new List<RunEvent>();
        await foreach (var e in channel.Reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            events.Add(e);
        }

        return events;
    }

    private static CompiledGraph<Job> Embedding(IEmbeddingGenerator<string, Embedding<float>> generator, params string[] texts) =>
        new StateGraph<Job>()
            .AddNode("Embed", async (job, ctx, ct) =>
            {
                var embeddings = await StrataEmbeddings.GenerateAsync(generator, texts, null, ctx, null, ct);
                return job with { Vectors = embeddings.Count };
            })
            .Compile();

    [Fact]
    public async Task A_Call_From_A_Node_Is_Observed_As_Embeddings_With_Its_Input_Tokens_Only()
    {
        var generator = new ScriptedEmbeddingGenerator("scripted", "embed-1", inputs =>
        {
            var embeddings = ScriptedEmbeddingGenerator.Embeddings("embed-1-v2", inputs.Select(_ => new float[] { 1, 0 }));
            embeddings.Usage!.OutputTokenCount = 3;
            return embeddings;
        });
        var channel = new RunEventChannel();

        var result = await Embedding(generator, "a", "b").RunAsync("embed", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.Equal(2, Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Vectors);
        Assert.Equal(["a", "b"], Assert.Single(generator.Calls));
        var events = await Drain(channel);
        var started = events.OfType<ModelCallStarted>().Single();
        Assert.Equal(("embeddings", "embed-1", "scripted", "Embed"), (started.Operation, started.Model, started.Provider, started.NodePath));
        var completed = events.OfType<ModelCallCompleted>().Single();
        Assert.Equal(ModelCallResult.Succeeded, completed.Result);
        Assert.Equal("embeddings", completed.Operation);
        Assert.Equal(new ModelUsage(8, null), completed.Usage);
        Assert.Null(completed.FinishReason);
        Assert.Equal(started.CallId, completed.CallId);
    }

    [Fact]
    public async Task A_Streaming_Run_Gets_Nothing_From_An_Embedding_Call()
    {
        var generator = ScriptedEmbeddingGenerator.ByLength();

        var (reader, completion) = Embedding(generator, "a").RunStreaming("embed-streamed", new Job(), new InMemoryCheckpointStore());
        var tokens = 0;
        await foreach (var chunk in reader.ReadAllAsync().WithCancellation(new CancellationTokenSource(Patience).Token))
        {
            tokens += chunk.Type == StreamChunkType.Token ? 1 : 0;
        }

        Assert.IsType<GraphExecutionResult<Job>.Completed>(await completion);
        Assert.Equal(0, tokens);
    }

    [Fact]
    public async Task The_Span_And_The_Metrics_Name_The_Operation_And_Carry_No_Output_Tokens()
    {
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var measurements = new ConcurrentBag<(string Instrument, double Value, Dictionary<string, object?> Tags)>();
        using var meters = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == StrataTelemetry.Name
                    && instrument.Name is "gen_ai.client.token.usage" or "gen_ai.client.operation.duration")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var map = new Dictionary<string, object?>();
            foreach (var tag in tags)
            {
                map[tag.Key] = tag.Value;
            }

            measurements.Add((instrument.Name, value, map));
        }

        meters.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
        meters.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
        meters.Start();
        var generator = ScriptedEmbeddingGenerator.ByLength(model: "embed-span");

        await Embedding(generator, "a", "b", "c").RunAsync("embed-span-thread", new Job(), new InMemoryCheckpointStore());

        var span = spans.Single(a => a.OperationName == "strata.model" && (string?)a.GetTagItem("strata.thread_id") == "embed-span-thread");
        Assert.Equal("embeddings embed-span", span.DisplayName);
        Assert.Equal("embeddings", span.GetTagItem("gen_ai.operation.name"));
        Assert.Equal("scripted", span.GetTagItem("gen_ai.provider.name"));
        Assert.Equal(12L, span.GetTagItem("gen_ai.usage.input_tokens"));
        Assert.Null(span.GetTagItem("gen_ai.usage.output_tokens"));
        Assert.Null(span.GetTagItem("gen_ai.request.max_tokens"));
        var mine = measurements.Where(m => Equals(m.Tags.GetValueOrDefault("gen_ai.request.model"), "embed-span")).ToList();
        Assert.All(mine, m => Assert.Equal("embeddings", m.Tags["gen_ai.operation.name"]));
        var usage = Assert.Single(mine, m => m.Instrument == "gen_ai.client.token.usage");
        Assert.Equal((12d, "input"), (usage.Value, usage.Tags["gen_ai.token.type"]));
        Assert.Single(mine, m => m.Instrument == "gen_ai.client.operation.duration");
    }

    [Fact]
    public async Task No_Texts_Is_No_Call_And_Nothing_Recorded()
    {
        var generator = ScriptedEmbeddingGenerator.ByLength();
        var channel = new RunEventChannel();

        var result = await Embedding(generator).RunAsync("embed-none", new Job(Vectors: -1), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.Equal(0, Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State.Vectors);
        Assert.Empty(generator.Calls);
        Assert.DoesNotContain(await Drain(channel), e => e is ModelCallStarted or ModelCallCompleted);
    }

    [Fact]
    public async Task The_Model_Is_The_Options_Then_The_Generators_Then_Unknown()
    {
        var named = ScriptedEmbeddingGenerator.ByLength(model: "embed-default");
        var anonymous = ScriptedEmbeddingGenerator.ByLength(provider: null, model: null);
        var spans = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == StrataTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);

        var asked = new EmbeddingGenerationOptions { ModelId = "embed-asked", Dimensions = 2 };
        await StrataEmbeddings.GenerateAsync(named, ["a"], asked);
        await StrataEmbeddings.GenerateAsync(named, ["a"]);
        await StrataEmbeddings.GenerateAsync(anonymous, ["a"]);

        Assert.Same(asked, named.Options.First());
        var models = spans.Select(s => (string?)s.GetTagItem("gen_ai.request.model")).ToList();
        Assert.Contains("embed-asked", models);
        Assert.Contains("embed-default", models);
        Assert.Contains(spans, s => s.DisplayName == "embeddings unknown" && s.GetTagItem("gen_ai.provider.name") is null);
    }

    [Fact]
    public async Task A_Rate_Limit_Is_Transient_With_Its_Hint_And_A_Bad_Request_Is_Fatal()
    {
        var limited = new ScriptedEmbeddingGenerator("scripted", "embed-1", _ =>
            throw new ModelCallException("slow down", isTransient: true, statusCode: 429, retryAfter: TimeSpan.FromSeconds(2)));
        var refused = new ScriptedEmbeddingGenerator("scripted", "embed-1", _ =>
            throw new HttpRequestException("bad input", null, HttpStatusCode.BadRequest));

        var transient = await Assert.ThrowsAsync<NodeException>(() => StrataEmbeddings.GenerateAsync(limited, ["a"]));
        var fatal = await Assert.ThrowsAsync<NodeException>(() => StrataEmbeddings.GenerateAsync(refused, ["a"]));

        Assert.Equal((NodeFailureKind.Transient, TimeSpan.FromSeconds(2)), (transient.Kind, transient.RetryAfter));
        Assert.Equal("model call failed (429): slow down", transient.Message);
        Assert.Equal(NodeFailureKind.Fatal, fatal.Kind);
        Assert.Equal("model call failed (400): bad input", fatal.Message);
    }

    [Fact]
    public async Task A_Failed_Call_In_A_Run_Is_Completed_Failed_And_Retried_After_The_Hint()
    {
        var attempts = 0;
        var generator = new ScriptedEmbeddingGenerator("scripted", "embed-1", inputs =>
            Interlocked.Increment(ref attempts) == 1
                ? throw new ModelCallException("slow down", isTransient: true, statusCode: 429, retryAfter: TimeSpan.FromMilliseconds(30))
                : ScriptedEmbeddingGenerator.Embeddings("embed-1", inputs.Select(_ => new float[] { 1 })));
        var graph = new StateGraph<Job>()
            .AddFallibleNode("Embed", async (job, ctx, ct) =>
                job with { Vectors = (await StrataEmbeddings.GenerateAsync(generator, ["a"], null, ctx, null, ct)).Count })
            .SetRecoveryPolicy("Embed", RecoveryPolicy.Retry(1).WithBackoff(TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(5)))
            .Compile();
        var channel = new RunEventChannel();

        var result = await graph.RunAsync("embed-retried", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        var events = await Drain(channel);
        Assert.Equal(TimeSpan.FromMilliseconds(30), events.OfType<RetryScheduled>().Single().Delay);
        var failed = events.OfType<ModelCallCompleted>().First();
        Assert.Equal((ModelCallResult.Failed, "slow down", "embeddings"), (failed.Result, failed.Error, failed.Operation));
        Assert.Equal(ModelUsage.None, failed.Usage);
    }

    [Fact]
    public async Task An_Answer_Short_Of_Vectors_Is_A_Fatal_Invalid_Response()
    {
        var generator = new ScriptedEmbeddingGenerator("scripted", "embed-1", _ =>
            ScriptedEmbeddingGenerator.Embeddings("embed-1", [[1, 0]]));
        var channel = new RunEventChannel();
        NodeException? thrown = null;
        var graph = new StateGraph<Job>()
            .AddNode("Embed", async (job, ctx, ct) =>
            {
                thrown = await Assert.ThrowsAsync<NodeException>(() => StrataEmbeddings.GenerateAsync(generator, ["a", "b"], null, ctx, null, ct));
                return job;
            })
            .Compile();

        await graph.RunAsync("embed-short", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.Equal(NodeFailureKind.Fatal, thrown!.Kind);
        Assert.Equal("model call failed (invalid_response): expected 2 vectors, the embedder returned 1", thrown.Message);
        var completed = (await Drain(channel)).OfType<ModelCallCompleted>().Single();
        Assert.Equal((ModelCallResult.Failed, "expected 2 vectors, the embedder returned 1"), (completed.Result, completed.Error));
    }

    [Fact]
    public async Task A_Generators_Own_Node_Exception_Passes_Through()
    {
        var generator = new ScriptedEmbeddingGenerator("scripted", "embed-1", _ => throw NodeException.Fatal("quota spent"));

        var thrown = await Assert.ThrowsAsync<NodeException>(() => StrataEmbeddings.GenerateAsync(generator, ["a"]));

        Assert.Equal("quota spent", thrown.Message);
    }

    [Fact]
    public async Task A_Cancelled_Call_Is_Rethrown_And_Completed_As_Cancelled()
    {
        var generator = new ScriptedEmbeddingGenerator("scripted", "embed-1", _ => throw new InvalidOperationException("never"))
        {
            Hang = true,
        };
        var channel = new RunEventChannel();
        var graph = new StateGraph<Job>()
            .AddNode("Embed", async (job, ctx, ct) =>
            {
                using var call = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var embedding = StrataEmbeddings.GenerateAsync(generator, ["a"], null, ctx, null, call.Token);
                while (generator.Calls.IsEmpty)
                {
                    await Task.Delay(5, ct);
                }

                await call.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => embedding);
                return job;
            })
            .Compile();

        var result = await graph.RunAsync("embed-cancel", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        var completed = (await Drain(channel)).OfType<ModelCallCompleted>().Single();
        Assert.Equal((ModelCallResult.Cancelled, "embeddings"), (completed.Result, completed.Operation));
    }

    // --- The memory helper, beyond the shared cases ---

    [Fact]
    public void The_Label_Is_The_Option_Else_Provider_Slash_Model_And_A_Generator_Without_A_Model_Needs_One()
    {
        var store = new InMemoryMemoryStore(3);

        Assert.Equal("scripted/embed-1", new StrataMemory(store, ScriptedEmbeddingGenerator.ByLength()).Model);
        Assert.Equal("unknown/embed-1", new StrataMemory(store, ScriptedEmbeddingGenerator.ByLength(provider: null)).Model);
        Assert.Equal(
            "nomic-768",
            new StrataMemory(store, ScriptedEmbeddingGenerator.ByLength(model: null), new StrataMemoryOptions { Model = "nomic-768" }).Model);
        var refused = Assert.Throws<InvalidOperationException>(() => new StrataMemory(store, ScriptedEmbeddingGenerator.ByLength(model: null)));
        Assert.Equal("the embedding generator names no model: label the memory's vectors with StrataMemoryOptions.Model", refused.Message);
        Assert.Same(store, new StrataMemory(store, ScriptedEmbeddingGenerator.ByLength()).Store);
    }

    [Fact]
    public void A_Blank_Label_Is_Refused_And_A_Blank_Default_Model_Is_No_Model()
    {
        var store = new InMemoryMemoryStore(3);

        foreach (var blank in (string[])["", "  "])
        {
            var refused = Assert.Throws<ArgumentException>(() =>
                new StrataMemory(store, ScriptedEmbeddingGenerator.ByLength(), new StrataMemoryOptions { Model = blank }));
            Assert.Equal("the model is empty", refused.Message);
            Assert.Throws<InvalidOperationException>(() => new StrataMemory(store, ScriptedEmbeddingGenerator.ByLength(model: blank)));
            Assert.Equal(
                "label",
                new StrataMemory(store, ScriptedEmbeddingGenerator.ByLength(model: blank), new StrataMemoryOptions { Model = "label" }).Model);
        }
    }

    [Fact]
    public async Task A_Blank_Model_Name_Falls_Through_To_The_Default_Then_Unknown()
    {
        var channel = new RunEventChannel();
        var chat = new ScriptedChatClient(Scripted.Text("ok"));
        var blankDefault = ScriptedEmbeddingGenerator.ByLength(model: "");
        var named = ScriptedEmbeddingGenerator.ByLength(model: "embed-default");
        var graph = new StateGraph<Job>()
            .AddNode("Calls", async (job, ctx, ct) =>
            {
                await StrataEmbeddings.GenerateAsync(blankDefault, ["a"], new EmbeddingGenerationOptions { ModelId = " " }, ctx, null, ct);
                await StrataEmbeddings.GenerateAsync(named, ["a"], new EmbeddingGenerationOptions { ModelId = "" }, ctx, null, ct);
                await StrataChat.GetResponseAsync(chat, [new(ChatRole.User, "?")], new ChatOptions { ModelId = " " }, ctx, null, ct);
                return job;
            })
            .Compile();

        var result = await graph.RunAsync("blank-models", new Job(), new InMemoryCheckpointStore(), new RunOptions { Observer = channel });

        Assert.IsType<GraphExecutionResult<Job>.Completed>(result);
        Assert.Equal(
            ["unknown", "embed-default", ScriptedChatClient.Model],
            (await Drain(channel)).OfType<ModelCallStarted>().Select(s => s.Model));
    }

    [Fact]
    public async Task The_Embedding_Request_Names_No_Model_And_A_Store_Failure_Is_The_Stores()
    {
        var generator = ScriptedEmbeddingGenerator.ByLength();
        var memory = new StrataMemory(new FailingStore(), generator);

        var failure = await Assert.ThrowsAsync<MemoryStoreException>(() => memory.RememberAsync(new MemoryEntry("incidents", "inc-1", "abc")));
        await Assert.ThrowsAsync<MemoryStoreException>(() => memory.RecallAsync("incidents", "abc"));

        Assert.Equal("memory upsert failed: unreachable", failure.Message);
        Assert.All(generator.Options, Assert.Null);
        Assert.Equal(2, generator.Calls.Count);
    }

    /// <summary>A store whose every write and search fails, as one that can't be reached does.</summary>
    private sealed class FailingStore : IMemoryStore
    {
        public int Dimensions => 3;

        public ValueTask UpsertAsync(MemoryRecord record, CancellationToken cancellationToken = default) =>
            throw new MemoryStoreException("memory upsert failed: unreachable");

        public ValueTask UpsertManyAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken = default) =>
            throw new MemoryStoreException("memory upsert failed: unreachable");

        public ValueTask<MemoryRecord?> GetAsync(MemoryKey key, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult<MemoryRecord?>(null);

        public ValueTask<bool> DeleteAsync(MemoryKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(false);

        public ValueTask<IReadOnlyList<MemoryMatch>> SearchAsync(MemorySearch search, CancellationToken cancellationToken = default) =>
            throw new MemoryStoreException("memory search failed: unreachable");

        public ValueTask<MemoryPage> ListAsync(MemoryQuery query, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(new MemoryPage([], null));
    }
}
