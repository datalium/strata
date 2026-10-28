using Datalium.Strata.Storage.Postgres;
using Npgsql;
using StackExchange.Redis;

namespace Datalium.Strata.Tests.Backends;

/// <summary>
/// Access to the live Postgres and Redis the backend tests need. They read
/// <c>STRATA_TEST_POSTGRES_URL</c> / <c>STRATA_TEST_REDIS_URL</c> (see the repo's
/// <c>docker-compose.yml</c>) -- the same variables the Rust tests read -- and are reported as
/// <b>skipped</b> when a variable is unset, so a machine without the services still gets a green
/// <c>dotnet test</c>. CI sets <c>STRATA_REQUIRE_BACKENDS=1</c>, which stops them skipping: an unset
/// variable then fails the test instead, so a misconfigured job can't quietly stop testing the adapters.
/// </summary>
internal static class TestBackends
{
    public const string PostgresVar = "STRATA_TEST_POSTGRES_URL";
    public const string RedisVar = "STRATA_TEST_REDIS_URL";

    private static bool Required => Environment.GetEnvironmentVariable("STRATA_REQUIRE_BACKENDS") == "1";

    /// <summary>The reason to skip a test that needs <paramref name="variable"/>, or <c>null</c> to run it.</summary>
    public static string? SkipReason(string variable) =>
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable(variable)) && !Required
            ? $"{variable} is not set (start `docker compose up -d --wait`)"
            : null;

    /// <summary>The URL in <paramref name="variable"/>; fails the test if it is unset (only reachable when <c>STRATA_REQUIRE_BACKENDS=1</c>).</summary>
    public static string Url(string variable)
    {
        var url = Environment.GetEnvironmentVariable(variable);
        return string.IsNullOrEmpty(url)
            ? throw new InvalidOperationException($"{variable} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run")
            : url;
    }

    public static string PostgresUrl => Url(PostgresVar);

    public static string RedisUrl => Url(RedisVar);
}

/// <summary>A test that needs the live Postgres from <see cref="TestBackends.PostgresVar"/>.</summary>
public sealed class PostgresFactAttribute : FactAttribute
{
    public PostgresFactAttribute() => Skip = TestBackends.SkipReason(TestBackends.PostgresVar);
}

/// <summary>A theory that needs the live Postgres from <see cref="TestBackends.PostgresVar"/>.</summary>
public sealed class PostgresTheoryAttribute : TheoryAttribute
{
    public PostgresTheoryAttribute() => Skip = TestBackends.SkipReason(TestBackends.PostgresVar);
}

/// <summary>A test that needs the live Redis from <see cref="TestBackends.RedisVar"/>.</summary>
public sealed class RedisFactAttribute : FactAttribute
{
    public RedisFactAttribute() => Skip = TestBackends.SkipReason(TestBackends.RedisVar);
}

/// <summary>A theory that needs the live Redis from <see cref="TestBackends.RedisVar"/>.</summary>
public sealed class RedisTheoryAttribute : TheoryAttribute
{
    public RedisTheoryAttribute() => Skip = TestBackends.SkipReason(TestBackends.RedisVar);
}

/// <summary>
/// A throwaway Postgres schema: stores created through <see cref="CreateDataSource"/> put their tables in
/// it (<c>Search Path</c>), so nothing touches <c>public</c>. Every test gets its own, so tests are
/// independent, can run in parallel, and can be re-run against a database that already has data in it.
/// </summary>
internal sealed class PostgresSandbox : IDisposable
{
    private readonly string _connectionString;
    private readonly List<NpgsqlDataSource> _dataSources = [];

    public PostgresSandbox(string label)
    {
        _connectionString = PostgresConnection.ToConnectionString(TestBackends.PostgresUrl);
        Schema = $"strata_test_{Environment.ProcessId}_{Guid.NewGuid():N}_{label}";
        using var admin = NpgsqlDataSource.Create(_connectionString);
        using var command = admin.CreateCommand($"CREATE SCHEMA {Schema}");
        command.ExecuteNonQuery();
    }

    public string Schema { get; }

    /// <summary>
    /// A pooled data source scoped to this sandbox's schema. With an <paramref name="applicationName"/>, its
    /// connections can be told from every other pool's, for a test that terminates them.
    /// </summary>
    public NpgsqlDataSource CreateDataSource(string? applicationName = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(_connectionString) { SearchPath = Schema };
        if (applicationName is not null)
        {
            builder.ApplicationName = applicationName;
        }

        var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        _dataSources.Add(dataSource);
        return dataSource;
    }

    /// <summary>
    /// <see cref="CreateDataSource"/> whose search path also reaches <c>public</c>, where pgvector's <c>vector</c>
    /// type lives (design/45-Long-Term-Memory.md). The extension is created there once per process first, so a
    /// memory store's migration finds it rather than creating it in this sandbox, whose drop would take it away
    /// from every other test.
    /// </summary>
    public NpgsqlDataSource CreateVectorDataSource()
    {
        lock (VectorGate)
        {
            if (!_vectorReady)
            {
                using var admin = NpgsqlDataSource.Create(_connectionString);
                using var command = admin.CreateCommand("CREATE EXTENSION IF NOT EXISTS vector SCHEMA public");
                try
                {
                    command.ExecuteNonQuery();
                }
                catch (PostgresException e) when (e.SqlState is PostgresErrorCodes.UniqueViolation or PostgresErrorCodes.DuplicateObject)
                {
                    // Another process created it at the same moment.
                }

                _vectorReady = true;
            }
        }

        var builder = new NpgsqlConnectionStringBuilder(_connectionString) { SearchPath = $"{Schema},public" };
        var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        _dataSources.Add(dataSource);
        return dataSource;
    }

    private static readonly Lock VectorGate = new();
    private static bool _vectorReady;

    /// <summary>
    /// A checkpoint store over <paramref name="dataSource"/> whose tables exist already, for tests that read them
    /// with their own SQL: a constructor alone no longer touches the database (design/34-Storage-Hardening.md).
    /// </summary>
    public static PostgresCheckpointStore Open(NpgsqlDataSource dataSource, PostgresStoreOptions? options = null)
    {
        var store = new PostgresCheckpointStore(dataSource, options);
        store.MigrateAsync().GetAwaiter().GetResult();
        return store;
    }

    /// <summary><see cref="Open"/> for a history store.</summary>
    public static PostgresHistoryStore OpenHistory(NpgsqlDataSource dataSource, PostgresStoreOptions? options = null)
    {
        var store = new PostgresHistoryStore(dataSource, options);
        store.MigrateAsync().GetAwaiter().GetResult();
        return store;
    }

    public void Dispose()
    {
        foreach (var dataSource in _dataSources)
        {
            dataSource.Dispose();
        }

        using var admin = NpgsqlDataSource.Create(_connectionString);
        using var command = admin.CreateCommand($"DROP SCHEMA IF EXISTS {Schema} CASCADE");
        command.ExecuteNonQuery();
    }
}

/// <summary>A unique Redis key prefix; every key under it is deleted on dispose.</summary>
internal sealed class RedisSandbox : IDisposable
{
    public RedisSandbox(string label)
    {
        Prefix = $"strata_test_{Environment.ProcessId}_{Guid.NewGuid():N}_{label}";
        Connection = Storage.Redis.RedisConnection.Connect(TestBackends.RedisUrl);
    }

    public string Prefix { get; }

    public ConnectionMultiplexer Connection { get; }

    public IDatabase Database => Connection.GetDatabase();

    public void Dispose()
    {
        var server = Connection.GetServer(Connection.GetEndPoints()[0]);
        var keys = server.Keys(pattern: $"{Prefix}:*").ToArray();
        if (keys.Length > 0)
        {
            Database.KeyDelete(keys);
        }

        Connection.Dispose();
    }
}
