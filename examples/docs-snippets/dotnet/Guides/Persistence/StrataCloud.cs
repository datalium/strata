// Samples for https://stratamesh.dev/guides/persistence/strata-cloud/
//
// Each sample has its own using lines, so each one is in a namespace block of its own. The store, run and decision
// worker samples talk to a live Strata Cloud: like strata's CloudLiveTests, they need STRATA_CLOUD_URL and
// STRATA_CLOUD_KEY, are skipped without them, and so are compiled but not run in CI. The hosting sample runs offline:
// it only builds and starts the host.

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.StrataCloud
{
    using Datalium.Strata.Graph;

    /// <summary>A test against a live Strata Cloud, skipped without <c>STRATA_CLOUD_URL</c> and <c>STRATA_CLOUD_KEY</c>.</summary>
    internal sealed class CloudLiveFactAttribute : FactAttribute
    {
        public CloudLiveFactAttribute()
        {
            var missing = new[] { "STRATA_CLOUD_URL", "STRATA_CLOUD_KEY" }
                .Where(name => string.IsNullOrEmpty(Environment.GetEnvironmentVariable(name)))
                .ToList();
            if (missing.Count > 0)
            {
                Skip = $"{string.Join(", ", missing)} not set (a live Strata Cloud)";
            }
        }
    }

    public sealed record Refund(string Customer, decimal Amount, string Status);

    /// <summary>The refund graph: <c>Prepare</c> waits for approval, then <c>Pay</c> runs.</summary>
    internal static class Refunds
    {
        public static CompiledGraph<Refund> Build() =>
            new StateGraph<Refund>()
                .AddNode("Prepare", refund => refund with { Status = "awaiting approval" })
                .AddInterruptBoundary("Prepare")
                .AddNode("Pay", refund => refund with { Status = "paid" })
                .AddEdge("Prepare", "Pay")
                .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.StrataCloud
{
    // docs:begin guides/persistence/strata-cloud/create
    using Datalium.Strata.Cloud;

    // docs:hide
    public partial class StrataCloudSamples
    {
        [CloudLiveFact]
        public async Task Create()
        {
            // docs:show
            var cloud = new StrataCloudOptions
            {
                BaseUrl = new Uri(Environment.GetEnvironmentVariable("STRATA_CLOUD_URL")!),  // <your beta endpoint>
                ApiKey = Environment.GetEnvironmentVariable("STRATA_CLOUD_KEY")!,
                RequestTimeout = TimeSpan.FromSeconds(10),
            };

            using var store = new StrataCloudCheckpointStore(cloud);
            // docs:end guides/persistence/strata-cloud/create

            Assert.Null(await store.LoadAsync($"docs-cloud-create-{Guid.NewGuid():N}"));

            // "The store is async only".
            Assert.Throws<NotSupportedException>(() => store.Load("docs-cloud-create"));
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.StrataCloud
{
    using Datalium.Strata.Cloud;
    using Datalium.Strata.Interruption;

    public partial class StrataCloudSamples
    {
        private static StrataCloudOptions Live => new()
        {
            BaseUrl = new Uri(Environment.GetEnvironmentVariable("STRATA_CLOUD_URL")!),
            ApiKey = Environment.GetEnvironmentVariable("STRATA_CLOUD_KEY")!,
            RequestTimeout = TimeSpan.FromSeconds(10),
        };

        [CloudLiveFact]
        public async Task Run()
        {
            using var store = new StrataCloudCheckpointStore(Live);
            var graph = Refunds.Build();
            var entitled = true;

            // docs:begin guides/persistence/strata-cloud/run
            try
            {
                var result = await graph.RunAsync("refund-42", new Refund("ada", 120m, "new"), store);
                Console.WriteLine(result.GetType().Name);   // Interrupted: the thread waits for a decision
            }
            catch (StrataCloudEntitlementException)
            {
                // Your plan's limit refused a new thread. Threads already running carry on.
                // docs:hide
                entitled = false;
                // docs:show
            }
            catch (StrataCloudException error)
            {
                Console.WriteLine($"{error.Kind} after {error.Attempts} attempts (HTTP {error.StatusCode}, {error.Code})");
                // docs:hide
                throw;
                // docs:show
            }
            // docs:end guides/persistence/strata-cloud/run

            if (entitled)
            {
                Assert.Equal(CheckpointStatus.Interrupted, (await store.LoadAsync("refund-42"))!.Status);
            }
        }

        [CloudLiveFact]
        public async Task ResumeDecisions()
        {
            var cloud = Live;
            using var store = new StrataCloudCheckpointStore(cloud);
            var graph = Refunds.Build();
            using var stopping = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var stoppingToken = stopping.Token;
            var handledAny = new List<HandledDecision<Refund>>();

            // docs:begin guides/persistence/strata-cloud/decision-worker
            using var decisions = new StrataCloudDecisions(cloud);
            var worker = new DecisionWorker<Refund>(graph, store, decisions, new DecisionWorkerOptions<Refund>
            {
                ThreadIdPrefix = "refund-",
                OnHandled = (handled, ct) =>
                {
                    Console.WriteLine($"{handled.Decision.ThreadId}: {handled.Outcome} (decided by {handled.Decision.DecidedBy})");
                    // docs:hide
                    handledAny.Add(handled);
                    // docs:show
                    return ValueTask.CompletedTask;
                },
            });

            await worker.RunAsync(stoppingToken);   // polls and resumes until the token is cancelled
            // docs:end guides/persistence/strata-cloud/decision-worker

            // It ran until the token was cancelled, and handled only the refund threads.
            Assert.True(stoppingToken.IsCancellationRequested);
            Assert.All(handledAny, handled => Assert.StartsWith("refund-", handled.Decision.ThreadId, StringComparison.Ordinal));
        }
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Persistence.StrataCloud
{
    using Datalium.Strata.Cloud;
    using Datalium.Strata.Hosting;
    using Datalium.Strata.Interruption;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Options;

    public partial class StrataCloudSamples
    {
        private static readonly string AppSettings =
            Path.Combine(AppContext.BaseDirectory, "files", "guides", "persistence", "strata-cloud", "appsettings.json");

        [Fact]
        public async Task Host()
        {
            // The file as the page shows it.
            var file = new ConfigurationBuilder().AddJsonFile(AppSettings).Build();
            Assert.Equal("<your beta endpoint>", file["Strata:Cloud:BaseUrl"]);

            // The endpoint is a port nothing listens on, and the key comes from outside the file, as
            // Strata__Cloud__ApiKey would give it.
            var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
            builder.Configuration.AddJsonFile(AppSettings);
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Strata:Cloud:BaseUrl"] = "http://127.0.0.1:9",
                ["Strata:Cloud:ApiKey"] = "sk-docs",
            });

            // docs:begin guides/persistence/strata-cloud/host
            builder.Services.AddStrata()
                .AddCloudCheckpoints()                       // Strata:Cloud
                .AddGraph(_ => Refunds.Build())
                .AddWakeDriver<Refund>()
                .AddRecoveryDriver<Refund>()
                .AddDecisionWorker<Refund>(_ => new DecisionWorkerOptions<Refund> { ThreadIdPrefix = "refund-" });
            // docs:end guides/persistence/strata-cloud/host

            using var host = builder.Build();
            Assert.Equal(TimeSpan.FromSeconds(10), host.Services.GetRequiredService<IOptions<StrataCloudHostOptions>>().Value.RequestTimeout);
            var lease = host.Services.GetRequiredService<IOptions<StrataRunOptions>>().Value.Lease;
            Assert.Equal(("refunds-1", TimeSpan.FromSeconds(30)), (lease.Owner, lease.Ttl));
            Assert.IsType<StrataCloudCheckpointStore>(host.Services.GetRequiredService<ICheckpointStore>());
            Assert.NotNull(host.Services.GetRequiredService<StrataCloudDecisions>());

            // The host starts with nothing to reach (its checks send nothing to Cloud), and stops.
            await host.StartAsync();
            await host.StopAsync();
        }
    }
}
