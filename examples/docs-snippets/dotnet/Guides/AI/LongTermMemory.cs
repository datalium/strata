// Samples for https://stratamesh.dev/guides/ai/long-term-memory/
//
// A sample that starts with using lines has a namespace block of its own: using directives can only open a
// namespace, so each block's are the ones its sample shows, after the ones the test around it needs.
//
// The stores sample needs STRATA_TEST_POSTGRES_URL (with pgvector) and STRATA_TEST_REDIS_URL (Redis 8). Without them
// it is skipped, unless STRATA_REQUIRE_BACKENDS=1. Its Postgres tables go in a schema of their own and its Redis keys
// under a prefix of their own, both removed afterwards.

namespace Datalium.Strata.DocsSnippets.Guides.AI.LongTermMemory
{
    using Datalium.Strata.AI;
    using Datalium.Strata.Memory;
    // docs:begin guides/ai/long-term-memory/embedder
    using System.ClientModel;
    using System.ClientModel.Primitives;
    using Microsoft.Extensions.AI;
    using OpenAI;

    // docs:hide
    public partial class LongTermMemorySamples
    {
        [Fact]
        public void CreateAnEmbedder()
        {
            var apiKey = "sk-docs-snippets";
            // docs:show
            IEmbeddingGenerator<string, Embedding<float>> embedder =
                new OpenAI.Embeddings.EmbeddingClient(
                    "text-embedding-3-small",
                    new ApiKeyCredential(apiKey),
                    new OpenAIClientOptions { RetryPolicy = new ClientRetryPolicy(maxRetries: 0) })
                .AsIEmbeddingGenerator();
            // docs:end guides/ai/long-term-memory/embedder

            // The labels the page gives: <provider>/<model>, here for OpenAI and for the SDK pointed at Ollama.
            Assert.Equal("openai/text-embedding-3-small", new StrataMemory(new InMemoryMemoryStore(1536), embedder).Model);
            IEmbeddingGenerator<string, Embedding<float>> ollama =
                new OpenAI.Embeddings.EmbeddingClient(
                    "nomic-embed-text",
                    new ApiKeyCredential("unused"),
                    new OpenAIClientOptions { Endpoint = new Uri("http://localhost:11434/v1") })
                .AsIEmbeddingGenerator();
            Assert.Equal("openai/nomic-embed-text", new StrataMemory(new InMemoryMemoryStore(768), ollama).Model);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.AI.LongTermMemory
{
    using Microsoft.Data.Sqlite;
    using Npgsql;
    using StackExchange.Redis;
    // docs:begin guides/ai/long-term-memory/stores
    using Datalium.Strata.Memory;
    using Datalium.Strata.Storage;
    using Datalium.Strata.Storage.Postgres;
    using Datalium.Strata.Storage.Redis;
    using Datalium.Strata.Storage.Sqlite;

    // docs:hide
    public partial class LongTermMemorySamples
    {
        [PostgresAndRedisFact]
        public async Task CreateAMemoryStore()
        {
            // pgvector goes in public; the store's table goes in a schema of this test's own.
            var name = $"docs_long_term_memory_{Environment.ProcessId}_{Guid.NewGuid():N}";
            var server = PostgresConnection.ToConnectionString(Backends.Url(Backends.PostgresVar));
            await using (var admin = NpgsqlDataSource.Create(server))
            {
                try
                {
                    await admin.CreateCommand("CREATE EXTENSION IF NOT EXISTS vector SCHEMA public").ExecuteNonQueryAsync();
                }
                catch (PostgresException e) when (e.SqlState is PostgresErrorCodes.UniqueViolation or PostgresErrorCodes.DuplicateObject)
                {
                    // Another process created it at the same moment.
                }

                await admin.CreateCommand($"CREATE SCHEMA {name}").ExecuteNonQueryAsync();
            }

            var connectionString = new NpgsqlConnectionStringBuilder(server) { SearchPath = $"{name},public" }.ConnectionString;
            var redisUrl = Backends.Url(Backends.RedisVar);
            var prefix = name;
            var opened = new List<IMemoryStore>();
            try
            {
                // docs:show
                // In memory: for tests and development. It's gone with the process.
                IMemoryStore store = new InMemoryMemoryStore(dimensions: 1536);
                // docs:hide
                opened.Add(store);
                // docs:show

                // SQLite with sqlite-vec
                store = await SqliteMemoryStore.OpenAsync("memory.db", new SqliteMemoryOptions { Dimensions = 1536 });
                // docs:hide
                opened.Add(store);
                // docs:show

                // Postgres with pgvector, here with an HNSW index
                store = await PostgresMemoryStore.OpenAsync(
                    connectionString, new PostgresMemoryOptions { Dimensions = 1536, Index = MemoryIndex.Hnsw() });
                // docs:hide
                opened.Add(store);
                // docs:show

                // Redis 8 or Redis Stack
                store = await RedisMemoryStore.ConnectAsync(redisUrl, new RedisMemoryOptions
                {
                    Dimensions = 1536,
                    // docs:hide
                    Prefix = prefix,
                    // docs:show
                });
                // docs:end guides/ai/long-term-memory/stores
                opened.Add(store);

                Assert.Equal(
                    [typeof(InMemoryMemoryStore), typeof(SqliteMemoryStore), typeof(PostgresMemoryStore), typeof(RedisMemoryStore)],
                    opened.Select(s => s.GetType()));
                Assert.All(opened, s => Assert.Equal(1536, s.Dimensions));
            }
            finally
            {
                foreach (var store in opened.OfType<IAsyncDisposable>())
                {
                    await store.DisposeAsync();
                }

                SqliteConnection.ClearAllPools();
                File.Delete("memory.db");

                await using (var admin = NpgsqlDataSource.Create(server))
                {
                    await admin.CreateCommand($"DROP SCHEMA IF EXISTS {name} CASCADE").ExecuteNonQueryAsync();
                }

                using var redis = await RedisConnection.ConnectAsync(redisUrl);
                var database = redis.GetDatabase();
                try
                {
                    await database.ExecuteAsync("FT.DROPINDEX", $"{prefix}:memory:idx");
                }
                catch (RedisServerException)
                {
                    // The store never made it.
                }

                var keys = redis.GetServer(redis.GetEndPoints()[0]).Keys(pattern: $"{prefix}:*").ToArray();
                if (keys.Length > 0)
                {
                    await database.KeyDeleteAsync(keys);
                }
            }
        }
    }

    /// <summary>
    /// The live Postgres and Redis, as the strata tests reach them: skipped when a variable is unset, failed when
    /// STRATA_REQUIRE_BACKENDS=1 says they must run.
    /// </summary>
    internal static class Backends
    {
        public const string PostgresVar = "STRATA_TEST_POSTGRES_URL";
        public const string RedisVar = "STRATA_TEST_REDIS_URL";

        private static bool Required => Environment.GetEnvironmentVariable("STRATA_REQUIRE_BACKENDS") == "1";

        public static string? SkipReason(params string[] variables) =>
            variables.FirstOrDefault(v => string.IsNullOrEmpty(Environment.GetEnvironmentVariable(v))) is { } missing && !Required
                ? $"{missing} is not set (start `docker compose up -d --wait`)"
                : null;

        public static string Url(string variable)
        {
            var url = Environment.GetEnvironmentVariable(variable);
            return string.IsNullOrEmpty(url)
                ? throw new InvalidOperationException($"{variable} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run")
                : url;
        }
    }

    /// <summary>A test that needs both the live Postgres and the live Redis.</summary>
    internal sealed class PostgresAndRedisFactAttribute : FactAttribute
    {
        public PostgresAndRedisFactAttribute() => Skip = Backends.SkipReason(Backends.PostgresVar, Backends.RedisVar);
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.AI.LongTermMemory
{
    using Datalium.Strata.Memory;
    // docs:begin guides/ai/long-term-memory/helper
    using Datalium.Strata.AI;

    // docs:hide
    public partial class LongTermMemorySamples
    {
        [Fact]
        public void CreateTheHelper()
        {
            var store = new InMemoryMemoryStore(WordHashEmbedder.Dimensions);
            var embedder = new WordHashEmbedder();
            // docs:show
            var memory = new StrataMemory(store, embedder);
            // docs:end guides/ai/long-term-memory/helper

            Assert.Equal("test/word-hash", memory.Model);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.AI.LongTermMemory
{
    using System.Globalization;
    using Datalium.Strata.AI;
    using Datalium.Strata.Graph;
    using Datalium.Strata.Interruption;
    using Datalium.Strata.Memory;
    using Datalium.Strata.Storage;
    using Datalium.Strata.Tools;
    // docs:begin guides/ai/long-term-memory/remember
    using System.Text.Json.Nodes;

    // docs:hide
    public partial class LongTermMemorySamples
    {
        [Fact]
        public async Task RememberAndRecall()
        {
            var memory = new StrataMemory(new InMemoryMemoryStore(WordHashEmbedder.Dimensions), new WordHashEmbedder());
            // docs:show
            await memory.RememberManyAsync(
            [
                new MemoryEntry("incidents", "inc-101", "Checkout pods crash looping, out of memory after deploy")
                {
                    Tenant = "acme",
                    Content = new JsonObject { ["rootCause"] = "a memory leak in v2.3", ["action"] = "roll back the deploy" },
                    Metadata = new Dictionary<string, string> { ["service"] = "checkout" },
                },
                new MemoryEntry("incidents", "inc-103", "Search pods out of memory during the nightly reindex")
                {
                    Tenant = "acme",
                    Content = new JsonObject { ["rootCause"] = "a reindex batch too large", ["action"] = "halve the batch" },
                    Metadata = new Dictionary<string, string> { ["service"] = "search" },
                },
                // docs:hide ... inc-102 for acme, and inc-101 for globex
                new MemoryEntry("incidents", "inc-102", "Checkout latency spike, database connection pool exhausted")
                {
                    Tenant = "acme",
                    Content = new JsonObject { ["rootCause"] = "a connection pool too small for the sale", ["action"] = "raise the pool size" },
                    Metadata = new Dictionary<string, string> { ["service"] = "checkout" },
                },
                new MemoryEntry("incidents", "inc-101", "Checkout pods crash looping, out of memory after deploy")
                {
                    Tenant = "globex",
                    Content = new JsonObject { ["rootCause"] = "a noisy neighbour on the node", ["action"] = "move the pods to another node pool" },
                    Metadata = new Dictionary<string, string> { ["service"] = "checkout" },
                },
                // docs:show
            ]);
            // docs:end guides/ai/long-term-memory/remember

            // docs:begin guides/ai/long-term-memory/recall
            var matches = await memory.RecallAsync(
                "incidents", "pods out of memory", new RecallOptions { Tenant = "acme", Limit = 3 });
            foreach (var match in matches)
            {
                Console.WriteLine($"{match.Record.Id} {match.Similarity:0.000} {match.Record.Content?["action"]}");
            }
            // docs:end guides/ai/long-term-memory/recall

            // What the page says both languages print.
            Assert.Equal(
                ["inc-103 0.577 halve the batch", "inc-101 0.535 roll back the deploy", "inc-102 0.000 raise the pool size"],
                matches.Select(m => string.Create(
                    CultureInfo.InvariantCulture, $"{m.Record.Id} {m.Similarity:0.000} {m.Record.Content?["action"]}")));
        }

        [Fact]
        public async Task UseMemoryInANode()
        {
            var memory = await IncidentsAsync();
            // docs:begin guides/ai/long-term-memory/node
            var desk = new StateGraph<Incident>()
                .AddNode("Recall", async (incident, ctx, ct) =>
                {
                    // No tenant here: StrataMemory reads it from the run's metadata.
                    var matches = await memory.RecallAsync("incidents", incident.Symptoms, new RecallOptions
                    {
                        Limit = 3,
                        MinSimilarity = 0.5,
                        Exclude = [incident.Id],
                    }, ctx, ct);
                    return incident with
                    {
                        Similar = [.. matches.Select(m => $"{m.Record.Id} ({m.Similarity:0.00})")],
                        Action = matches is [var closest, ..]
                            ? closest.Record.Content!["action"]!.GetValue<string>()
                            : "page the on-call engineer",
                    };
                })
                .AddNode("Remember", async (incident, ctx, ct) =>
                {
                    await memory.RememberAsync(new MemoryEntry("incidents", incident.Id, incident.Symptoms)
                    {
                        Content = new JsonObject { ["action"] = incident.Action },
                        Metadata = new Dictionary<string, string> { ["service"] = incident.Service },
                    }, ctx, ct);
                    return incident;
                })
                .AddEdge("Recall", "Remember")
                .Compile();

            var result = await desk.RunAsync(
                "inc-201",
                new Incident("inc-201", "checkout", "Checkout pods out of memory again after this morning's deploy"),
                new InMemoryCheckpointStore(),
                new RunOptions { Metadata = { ["tenant"] = "acme" } });
            // docs:hide

            // Each tenant finds only its own inc-101.
            var acme = Assert.IsType<GraphExecutionResult<Incident>.Completed>(result).State;
            Assert.Equal(["inc-101 (0.67)"], acme.Similar);
            Assert.Equal("roll back the deploy", acme.Action);
            var globex = Assert.IsType<GraphExecutionResult<Incident>.Completed>(await desk.RunAsync(
                "inc-201",
                new Incident("inc-201", "checkout", "Checkout pods out of memory again after this morning's deploy"),
                new InMemoryCheckpointStore(),
                new RunOptions { Metadata = { ["tenant"] = "globex" } })).State;
            Assert.Equal(["inc-101 (0.67)"], globex.Similar);
            Assert.Equal("move the pods to another node pool", globex.Action);

            // A recall with no tenant reads only the records whose tenant is "": none here.
            Assert.Empty(await memory.RecallAsync("incidents", "pods out of memory"));

            // The tool, called for acme after the graph has run: inc-201 is the incident the graph remembered.
            var tools = new ToolRegistry().Register(new IncidentMemoryTools(memory));
            var answer = await tools.DispatchToolAsync(
                "incidents.recall",
                """{"symptoms":"pods out of memory"}""",
                ToolCallContext.None with { Metadata = new Dictionary<string, string> { ["tenant"] = "acme" } });
            Assert.Equal(
                "inc-103 (0.58): halve the batch\ninc-101 (0.53): roll back the deploy\ninc-201 (0.50): roll back the deploy",
                JsonNode.Parse(answer)!["result"]!.GetValue<string>());
        }

        /// <summary>A memory with the four incidents the remember sample stores, two of them elided on the page.</summary>
        private static async Task<StrataMemory> IncidentsAsync()
        {
            var memory = new StrataMemory(new InMemoryMemoryStore(WordHashEmbedder.Dimensions), new WordHashEmbedder());
            await memory.RememberManyAsync(
            [
                Past("acme", "inc-101", "checkout", "Checkout pods crash looping, out of memory after deploy", "roll back the deploy"),
                Past("acme", "inc-103", "search", "Search pods out of memory during the nightly reindex", "halve the batch"),
                Past("acme", "inc-102", "checkout", "Checkout latency spike, database connection pool exhausted", "raise the pool size"),
                Past("globex", "inc-101", "checkout", "Checkout pods crash looping, out of memory after deploy", "move the pods to another node pool"),
            ]);
            return memory;

            static MemoryEntry Past(string tenant, string id, string service, string symptoms, string action) =>
                new("incidents", id, symptoms)
                {
                    Tenant = tenant,
                    Content = new JsonObject { ["action"] = action },
                    Metadata = new Dictionary<string, string> { ["service"] = service },
                };
        }

        // docs:show

        sealed record Incident(string Id, string Service, string Symptoms)
        {
            public IReadOnlyList<string> Similar { get; init; } = [];
            public string Action { get; init; } = "";
        }
        // docs:end guides/ai/long-term-memory/node
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.AI.LongTermMemory
{
    using Datalium.Strata.AI;
    // docs:begin guides/ai/long-term-memory/tool
    using Datalium.Strata.Tools;

    public partial class IncidentMemoryTools(StrataMemory memory)
    {
        [StrataTool(Name = "incidents.recall", Description = "Past incidents most like the given symptoms, with what fixed them.")]
        public async Task<string> RecallAsync(string symptoms, ToolCallContext context, CancellationToken cancellationToken)
        {
            var matches = await memory.RecallAsync("incidents", symptoms, new RecallOptions
            {
                Tenant = context.Metadata.GetValueOrDefault("tenant", ""),
                Limit = 3,
            }, cancellationToken: cancellationToken);
            return string.Join("\n", matches.Select(m => $"{m.Record.Id} ({m.Similarity:0.00}): {m.Record.Content?["action"]}"));
        }
    }
    // docs:end guides/ai/long-term-memory/tool
}

namespace Datalium.Strata.DocsSnippets.Guides.AI.LongTermMemory
{
    using Datalium.Strata.AI;
    using Datalium.Strata.Memory;

    public partial class LongTermMemorySamples
    {
        [Fact]
        public async Task Forget()
        {
            // The records were written 400 days ago, by the store's clock.
            IMemoryStore store = new InMemoryMemoryStore(
                WordHashEmbedder.Dimensions, new FixedTime(DateTimeOffset.UtcNow.AddDays(-400)));
            await new StrataMemory(store, new WordHashEmbedder()).RememberManyAsync(
            [
                new MemoryEntry("incidents", "inc-101", "Checkout pods crash looping, out of memory after deploy") { Tenant = "acme" },
                new MemoryEntry("incidents", "inc-102", "Checkout latency spike, database connection pool exhausted") { Tenant = "acme" },
                new MemoryEntry("incidents", "inc-103", "Search pods out of memory during the nightly reindex") { Tenant = "acme" },
                new MemoryEntry("incidents", "inc-101", "Checkout pods crash looping, out of memory after deploy") { Tenant = "globex" },
            ]);

            // docs:begin guides/ai/long-term-memory/forget
            var deleted = await store.DeleteAsync(new MemoryKey("incidents", "acme", "inc-102"));

            if (store is IMemoryRetention retention)
            {
                var purged = await retention.PurgeAsync(
                    new MemoryPurge(DateTimeOffset.UtcNow.AddDays(-365)) { Namespace = "incidents" });
                // docs:hide
                // acme's inc-101 and inc-103 and globex's inc-101: every tenant, as the purge names none.
                Assert.Equal(3, purged);
                // docs:show
            }
            // docs:end guides/ai/long-term-memory/forget

            Assert.True(deleted);
            Assert.IsAssignableFrom<IMemoryRetention>(store);
            Assert.Empty((await store.ListAsync(new MemoryQuery("incidents") { Tenant = "acme" })).Records);
        }

        /// <summary>A clock that stays at one time.</summary>
        private sealed class FixedTime(DateTimeOffset now) : TimeProvider
        {
            public override DateTimeOffset GetUtcNow() => now;
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.AI.LongTermMemory
{
    using Datalium.Strata.AI;
    using Datalium.Strata.Hosting;
    using Microsoft.Data.Sqlite;
    using Microsoft.Extensions.AI;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;

    public partial class LongTermMemorySamples
    {
        [Fact]
        public async Task RunItInAHost()
        {
            var path = Path.Combine(Path.GetTempPath(), $"docs-long-term-memory-{Guid.NewGuid():N}.db");
            string[] args = [$"--Strata:Sqlite:Path={path}"];
            IEmbeddingGenerator<string, Embedding<float>> embedder = new WordHashEmbedder();
            StrataMemory? started = null;
            try
            {
                // docs:begin guides/ai/long-term-memory/host
                var builder = Host.CreateApplicationBuilder(args);

                builder.Services.AddSingleton(embedder);                     // an IEmbeddingGenerator<string, Embedding<float>>
                builder.Services.AddStrata()
                    .AddSqliteCheckpoints()                                   // Strata:Sqlite
                    .AddSqliteMemory(o => o.Dimensions = 1536)                // Strata:Memory, in the same file
                    .AddMemory()                                              // StrataMemory over the store and the generator
                    .AddMemoryRetention(o => o.OlderThan = TimeSpan.FromDays(365));

                using var host = builder.Build();
                // docs:hide
                // Once the host has started, take the helper from the container, and stop.
                var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
                lifetime.ApplicationStarted.Register(() =>
                {
                    started = host.Services.GetRequiredService<StrataMemory>();
                    lifetime.StopApplication();
                });
                // docs:show
                await host.RunAsync();
                // docs:end guides/ai/long-term-memory/host
            }
            finally
            {
                SqliteConnection.ClearAllPools();
                foreach (var file in new[] { path, $"{path}-wal", $"{path}-shm" })
                {
                    File.Delete(file);
                }
            }

            Assert.NotNull(started);
            Assert.Equal("test/word-hash", started.Model);
            Assert.Equal(1536, started.Store.Dimensions);
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.AI.LongTermMemory
{
    // docs:begin guides/ai/long-term-memory/word-hash
    using System.Text;
    using System.Text.RegularExpressions;
    using Microsoft.Extensions.AI;

    // Each word of four letters or more adds 1 to one of 64 buckets.
    sealed partial class WordHashEmbedder : IEmbeddingGenerator<string, Embedding<float>>
    {
        public const int Dimensions = 64;

        public Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
            IEnumerable<string> values, EmbeddingGenerationOptions? options = null, CancellationToken cancellationToken = default)
        {
            var embeddings = new GeneratedEmbeddings<Embedding<float>>();
            foreach (var text in values)
            {
                var vector = new float[Dimensions];
                foreach (Match word in Words().Matches(text.ToLowerInvariant()))
                {
                    vector[Bucket(word.Value)] += 1;
                }

                vector[0] += 0.01f; // never all zeros: the store refuses a vector with no direction
                embeddings.Add(new Embedding<float>(vector));
            }

            return Task.FromResult(embeddings);
        }

        // The label: test/word-hash.
        public object? GetService(Type serviceType, object? serviceKey = null) =>
            serviceType == typeof(EmbeddingGeneratorMetadata)
                ? new EmbeddingGeneratorMetadata("test", null, "word-hash", Dimensions)
                : null;

        public void Dispose()
        {
        }

        // FNV-1a: the same bucket in every process, unlike string.GetHashCode.
        private static int Bucket(string word)
        {
            var hash = 2166136261u;
            foreach (var b in Encoding.UTF8.GetBytes(word))
            {
                hash = (hash ^ b) * 16777619u;
            }

            return (int)(hash % Dimensions);
        }

        [GeneratedRegex("[a-z0-9]{4,}")]
        private static partial Regex Words();
    }
    // docs:end guides/ai/long-term-memory/word-hash
}
