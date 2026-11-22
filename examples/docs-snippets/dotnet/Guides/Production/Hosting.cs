// Samples for https://stratamesh.dev/guides/production/hosting/

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Datalium.Strata.Graph;
using Datalium.Strata.Hosting;
using Datalium.Strata.Interruption;
using Datalium.Strata.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace Datalium.Strata.DocsSnippets.Guides.Production.Hosting;

public class HostingSamples
{
    [Fact]
    public async Task UseTheGraph()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddStrata().AddInMemoryCheckpoints().AddGraph(_ => Refunds.Build());
        builder.Services.AddSingleton<RefundDesk>();
        using var host = builder.Build();
        await host.StartAsync();

        var desk = host.Services.GetRequiredService<RefundDesk>();
        var started = await desk.StartAsync("refund-1042", new Refund("acme", 120m, "new"));
        var approved = await desk.ApproveAsync("refund-1042", "jdoe");

        Assert.Equal("Prepare", Assert.IsType<GraphExecutionResult<Refund>.Interrupted>(started).SuspendedNode);
        Assert.Equal(new Refund("acme", 120m, "paid"), Assert.IsType<GraphExecutionResult<Refund>.Completed>(approved).State);
        await host.StopAsync();
    }

    [Fact]
    public async Task OpenTelemetry()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddStrata()
            .AddInMemoryCheckpoints()
            .AddInstrumentation()
            .AddGraph(_ => Refunds.Build());

        // docs:begin guides/production/hosting/opentelemetry
        builder.Services.AddOpenTelemetry()
            .WithTracing(tracing => tracing.AddConsoleExporter())
            .WithMetrics(metrics => metrics.AddConsoleExporter());
        // docs:end guides/production/hosting/opentelemetry

        // Beside the console: what the providers export, to check it.
        var spans = new List<Activity>();
        var metrics = new List<Metric>();
        builder.Services.ConfigureOpenTelemetryTracerProvider(tracing => tracing.AddInMemoryExporter(spans));
        builder.Services.ConfigureOpenTelemetryMeterProvider(meters => meters.AddInMemoryExporter(metrics));
        using var host = builder.Build();
        await host.StartAsync();

        const string thread = "hosting-otel-refund-1";
        var runner = host.Services.GetRequiredService<StrataRunner<Refund>>();
        await runner.RunAsync(thread, new Refund("acme", 120m, "new"));
        host.Services.GetRequiredService<TracerProvider>().ForceFlush();
        host.Services.GetRequiredService<MeterProvider>().ForceFlush();

        Assert.Contains(spans, span =>
            span.Source.Name == StrataTelemetry.Name
                && span.OperationName == "strata.run"
                && (string?)span.GetTagItem("strata.thread_id") == thread);
        Assert.Contains(metrics, metric => metric.MeterName == StrataTelemetry.Name && metric.Name == "strata.graphs.started");
        await host.StopAsync();
    }

    [Fact]
    public async Task Handler()
    {
        var dbPath = TempDatabase();
        try
        {
            // docs:begin guides/production/hosting/handler
            // docs:hide
            string[] args = [];
            // docs:show
            var builder = WebApplication.CreateBuilder(args);
            // docs:hide
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            UseThePageSettings(builder.Configuration, dbPath);
            builder.Logging.ClearProviders();
            // docs:show

            builder.Services.AddStrata()
                .AddSqliteCheckpoints()
                .AddHealthChecks()
                .AddGraph(_ => Refunds.Build())
                .AddWakeDriver<Refund>()
                .AddRecoveryDriver<Refund>();

            var app = builder.Build();

            app.MapPost("/refunds/{id}", async (string id, Refund refund, StrataRunner<Refund> runner) =>
                Describe(await runner.RunAsync(id, refund)));

            app.MapPost("/refunds/{id}/approve", async (string id, string reviewer, StrataRunner<Refund> runner) =>
                Describe(await runner.ResumeAsync(id, HumanSignal.Approve(reviewer))));

            app.MapHealthChecks("/health");
            // docs:hide
            var calls = CallTheHandlersAsync(app);
            // docs:show

            app.Run();
            // docs:hide
            var (started, approved, again, health) = await calls;
            Assert.Equal(HttpStatusCode.Accepted, started.Status);
            Assert.Equal("""{"waitingAt":"Prepare"}""", started.Body);
            Assert.Equal(HttpStatusCode.OK, approved.Status);
            Assert.Equal("paid", JsonDocument.Parse(approved.Body).RootElement.GetProperty("status").GetString());
            Assert.Equal(HttpStatusCode.Conflict, again.Status);
            Assert.Equal(HttpStatusCode.OK, health.Status);
            // docs:show

            static IResult Describe(GraphExecutionResult<Refund> result) => result switch
            {
                GraphExecutionResult<Refund>.Completed done => Results.Ok(done.State),
                GraphExecutionResult<Refund>.Interrupted waiting => Results.Accepted(value: new { waitingAt = waiting.SuspendedNode }),
                GraphExecutionResult<Refund>.Failed failed => Results.Conflict(new { error = failed.Message }),
                _ => Results.StatusCode(StatusCodes.Status503ServiceUnavailable),
            };
            // docs:end guides/production/hosting/handler
        }
        finally
        {
            DeleteDatabase(dbPath);
        }
    }

    [Fact]
    public void ShutdownTimeout()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());

        // docs:begin guides/production/hosting/shutdown
        builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromMinutes(2));
        // docs:end guides/production/hosting/shutdown

        using var host = builder.Build();
        Assert.Equal(TimeSpan.FromMinutes(2), host.Services.GetRequiredService<IOptions<HostOptions>>().Value.ShutdownTimeout);
    }

    private sealed record Response(HttpStatusCode Status, string Body);

    // Once the app listens: start a refund, approve it, approve it again, ask for health, then stop the app.
    private static Task<(Response Started, Response Approved, Response Again, Response Health)> CallTheHandlersAsync(WebApplication app)
    {
        var listening = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        app.Lifetime.ApplicationStarted.Register(listening.SetResult);
        return Task.Run(async () =>
        {
            try
            {
                await listening.Task.WaitAsync(TimeSpan.FromSeconds(30));
                using var http = new HttpClient { BaseAddress = new Uri(app.Urls.Single()) };
                var started = await SendAsync(http.PostAsJsonAsync("/refunds/hosting-refund-1", new Refund("acme", 120m, "new")));
                var approved = await SendAsync(http.PostAsync("/refunds/hosting-refund-1/approve?reviewer=jdoe", null));
                var again = await SendAsync(http.PostAsync("/refunds/hosting-refund-1/approve?reviewer=jdoe", null));
                var health = await SendAsync(http.GetAsync("/health"));
                return (started, approved, again, health);
            }
            finally
            {
                app.Lifetime.StopApplication();
            }
        });

        static async Task<Response> SendAsync(Task<HttpResponseMessage> sending)
        {
            using var response = await sending;
            return new Response(response.StatusCode, await response.Content.ReadAsStringAsync());
        }
    }

    // The page's appsettings.json, with the database in a temp file.
    private static void UseThePageSettings(ConfigurationManager configuration, string dbPath)
    {
        configuration.AddJsonFile(
            Path.Combine(AppContext.BaseDirectory, "files", "guides", "production", "hosting", "appsettings.json"),
            optional: false);
        configuration.AddInMemoryCollection(new Dictionary<string, string?> { ["Strata:Sqlite:Path"] = dbPath });
    }

    private static string TempDatabase() => Path.Combine(Path.GetTempPath(), $"strata-docs-hosting-{Guid.NewGuid():N}.db");

    private static void DeleteDatabase(string dbPath)
    {
        SqliteConnection.ClearAllPools();
        foreach (var suffix in (string[])["", "-wal", "-shm"])
        {
            File.Delete(dbPath + suffix);
        }
    }

    // Last in the class: its sample ends with the types after it.
    [Fact]
    public async Task Register()
    {
        var dbPath = TempDatabase();
        var seen = new Seen();
        try
        {
            // docs:begin guides/production/hosting/register
            // docs:hide
            string[] args = [];
            // docs:show
            var builder = Host.CreateApplicationBuilder(args);
            // docs:hide
            UseThePageSettings(builder.Configuration, dbPath);
            builder.Logging.ClearProviders();
            builder.Logging.AddProvider(seen);
            builder.Services.AddSingleton(seen);
            builder.Services.AddHostedService<StopOnceEveryDriverRan>();
            // docs:show

            builder.Services.AddStrata()
                .AddSqliteCheckpoints()                  // Strata:Sqlite
                .AddInstrumentation()
                .AddHealthChecks()
                .AddGraph(_ => Refunds.Build())          // compiled when the host starts
                .AddWakeDriver<Refund>()                 // Strata:Wake
                .AddRecoveryDriver<Refund>()             // Strata:Recovery
                .AddRetention();                         // Strata:Retention

            builder.Services.AddSingleton<RefundDesk>();

            using var host = builder.Build();
            await host.RunAsync();
            // docs:hide
            Assert.Null(seen.Failure);
            Assert.Equal(["recovery:Refund", "retention", "wake:Refund"], seen.Drivers);
            Assert.True(File.Exists(dbPath), "the SQLite store's file, from Strata:Sqlite:Path");

            // The settings from the page's appsettings.json.
            Assert.Equal(TimeSpan.FromSeconds(30), seen.LeaseTtl);
            Assert.Equal(TimeSpan.FromSeconds(5), seen.Wake?.Interval);
            Assert.Equal(TimeSpan.FromSeconds(30), seen.Recovery?.Interval);
            Assert.Equal(8, seen.Recovery?.MaxConcurrency);
            Assert.Equal(TimeSpan.FromHours(1), seen.Retention?.Interval);
            Assert.Equal(TimeSpan.FromDays(30), seen.Retention?.OlderThan);

            // One line per driver when the host is up.
            var lines = seen.Lines.ToList();
            Assert.Contains("Strata driver wake:Refund started, every 00:00:05", lines);
            Assert.Contains("Strata driver recovery:Refund started, every 00:00:30", lines);
            Assert.Contains("Strata driver retention started, every 01:00:00", lines);
        }
        finally
        {
            DeleteDatabase(dbPath);
        }
    }
}
// docs:show

public sealed record Refund(string Customer, decimal Amount, string Status);

public static class Refunds
{
    public static CompiledGraph<Refund> Build() =>
        new StateGraph<Refund>()
            .AddNode("Prepare", refund => refund with { Status = "awaiting approval" })
            .AddInterruptBoundary("Prepare")
            .AddNode("Pay", refund => refund with { Status = "paid" })
            .AddEdge("Prepare", "Pay")
            .Compile(new CompilationOptions { Checkpointing = CheckpointPolicy.EveryNode });
}
// docs:end guides/production/hosting/register

// docs:begin guides/production/hosting/use-graph
public sealed class RefundDesk(StrataRunner<Refund> runner)
{
    public Task<GraphExecutionResult<Refund>> StartAsync(string id, Refund refund, CancellationToken ct = default) =>
        runner.RunAsync(id, refund, cancellationToken: ct);

    public Task<GraphExecutionResult<Refund>> ApproveAsync(string id, string reviewer, CancellationToken ct = default) =>
        runner.ResumeAsync(id, HumanSignal.Approve(reviewer), cancellationToken: ct);
}
// docs:end guides/production/hosting/use-graph

/// <summary>What the register sample's host did: its drivers, its settings and the hosting package's log lines.</summary>
internal sealed class Seen : ILoggerProvider
{
    public ConcurrentQueue<string> Lines { get; } = new();

    public List<string> Drivers { get; set; } = [];

    public TimeSpan? LeaseTtl { get; set; }

    public WakeDriverOptions? Wake { get; set; }

    public RecoveryDriverOptions? Recovery { get; set; }

    public RetentionOptions? Retention { get; set; }

    public string? Failure { get; set; }

    public ILogger CreateLogger(string categoryName) => new Logger(categoryName, Lines);

    public void Dispose()
    {
    }

    private sealed class Logger(string category, ConcurrentQueue<string> lines) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) =>
            logLevel >= LogLevel.Information && category.StartsWith("Datalium.Strata.Hosting", StringComparison.Ordinal);

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
            {
                lines.Enqueue(formatter(state, exception));
            }
        }
    }
}

/// <summary>Stops the register sample's host, as Ctrl+C would, once every driver has finished a round.</summary>
internal sealed class StopOnceEveryDriverRan(
    IStrataDriverStatus status,
    IServiceProvider services,
    IHostApplicationLifetime lifetime,
    Seen seen) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (status.Drivers.Count < 3 || status.Drivers.Any(d => d.Rounds == 0))
            {
                if (DateTime.UtcNow > deadline)
                {
                    seen.Failure = $"the drivers didn't each run a round: {string.Join(", ", status.Drivers)}";
                    return;
                }

                await Task.Delay(10, stoppingToken);
            }

            seen.Drivers = [.. status.Drivers.Select(d => d.Name)];
            if (status.Drivers.Any(d => d.FailuresInARow > 0))
            {
                seen.Failure = $"a driver's round failed: {string.Join(", ", status.Drivers)}";
            }

            var graph = typeof(Refund).FullName;
            seen.LeaseTtl = services.GetRequiredService<StrataRunner<Refund>>().CreateOptions().Lease?.Ttl;
            seen.Wake = services.GetRequiredService<IOptionsMonitor<WakeDriverOptions>>().Get(graph);
            seen.Recovery = services.GetRequiredService<IOptionsMonitor<RecoveryDriverOptions>>().Get(graph);
            seen.Retention = services.GetRequiredService<IOptions<RetentionOptions>>().Value;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            seen.Failure = e.ToString();
        }
        finally
        {
            lifetime.StopApplication();
        }
    }
}
