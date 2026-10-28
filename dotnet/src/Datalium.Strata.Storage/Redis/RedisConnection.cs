using StackExchange.Redis;

namespace Datalium.Strata.Storage.Redis;

/// <summary>
/// Options of <see cref="RedisCheckpointStore"/> (<c>design/34-Storage-Hardening.md</c>). Mirrors
/// <c>strata-memory</c>'s <c>RedisStoreOptions</c>.
/// </summary>
public sealed record RedisStoreOptions
{
    /// <summary>Key namespace; keys are <c>{prefix}:checkpoints:…</c>.</summary>
    public string Prefix { get; init; } = RedisConnection.DefaultPrefix;

    /// <summary>How a call that failed for a transient reason is repeated. <see cref="StoreRetryPolicy.None"/> turns it off.</summary>
    public StoreRetryPolicy Retry { get; init; } = new();

    /// <summary>
    /// How long a thread's keys live after it completes or is ended: a save that leaves it finished gives its
    /// list that time to live, and a save that leaves it in any other state clears it, so a thread that runs
    /// again doesn't expire. <see langword="null"/> (the default): finished threads stay until they are purged.
    /// At least one millisecond.
    /// </summary>
    public TimeSpan? FinishedThreadTtl { get; init; }

    /// <summary>The clock for the waits between attempts. <see langword="null"/>: <see cref="System.TimeProvider.System"/>.</summary>
    public TimeProvider? TimeProvider { get; init; }

    /// <summary>
    /// Stores a <c>running</c> checkpoint as the changes to the thread's previous row, with a whole row every
    /// <see cref="DeltaOptions.FullEvery"/> (<c>design/41-Checkpoint-Efficiency.md</c>, "Deltas"). Off when
    /// <see langword="null"/>, the default. A store reads rows stored this way whatever its own options say.
    /// </summary>
    public DeltaOptions? Deltas { get; init; }

    /// <summary>
    /// Stores each top-level value of a level's state whose JSON is at least <see cref="OffloadOptions.Threshold"/>
    /// bytes once, in a blob store, and refers to it (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"). Off when
    /// <see langword="null"/>, the default. The blobs are read from where these options say, whatever the threshold:
    /// the store's own database when <see cref="OffloadOptions.Blobs"/> is <see langword="null"/>. The history store ignores it.
    /// </summary>
    public OffloadOptions? Offload { get; init; }

    /// <summary>
    /// For tests: called at named points of a call (<c>"save:after"</c>, once the save's script has run),
    /// where it may throw what a lost connection would.
    /// </summary>
    internal Func<string, ValueTask>? Fault { get; init; }
}

/// <summary>Which Redis failures a store repeats a call for (<c>design/34-Storage-Hardening.md</c>, "Retries").</summary>
internal static class RedisFaults
{
    public static bool IsTransient(Exception exception) => exception switch
    {
        RedisConnectionException or RedisTimeoutException => true,

        // The server is there, and can't answer yet.
        RedisServerException server =>
            server.Message.StartsWith("LOADING", StringComparison.Ordinal)
            || server.Message.StartsWith("BUSY", StringComparison.Ordinal)
            || server.Message.StartsWith("TRYAGAIN", StringComparison.Ordinal),
        _ => false,
    };

    /// <summary>Whether the command that failed with <paramref name="exception"/> never reached the server.</summary>
    public static bool NeverSent(Exception exception) => exception switch
    {
        RedisConnectionException connection =>
            connection.CommandStatus is CommandStatus.WaitingToBeSent or CommandStatus.WaitingInBacklog,
        RedisTimeoutException timeout => timeout.Commandstatus is CommandStatus.WaitingToBeSent or CommandStatus.WaitingInBacklog,
        _ => false,
    };
}

/// <summary>Connection handling shared by <see cref="RedisCheckpointStore"/> and <see cref="RedisHistoryStore"/>.</summary>
public static class RedisConnection
{
    /// <summary>The key namespace used when none is given. Equal to <c>DEFAULT_PREFIX</c> in <c>strata-memory</c>'s <c>redis_store.rs</c>.</summary>
    public const string DefaultPrefix = "strata";

    /// <summary>
    /// Connects to <paramref name="url"/>. Accepts the same <c>redis://[:password@]host[:port][/db]</c> URL
    /// the Rust adapters take (so one <c>STRATA_TEST_REDIS_URL</c> works for both runtimes), or a
    /// StackExchange.Redis configuration string such as <c>localhost:6379,password=...</c>. Fails after 5 s
    /// rather than hanging on an unreachable server.
    /// </summary>
    public static ConnectionMultiplexer Connect(string url)
    {
        var options = ToConfigurationOptions(url);
        options.ConnectTimeout = 5_000;
        options.AbortOnConnectFail = true;
        return ConnectionMultiplexer.Connect(options);
    }

    /// <summary><see cref="Connect"/> without blocking a thread on the connection.</summary>
    public static Task<ConnectionMultiplexer> ConnectAsync(string url)
    {
        var options = ToConfigurationOptions(url);
        options.ConnectTimeout = 5_000;
        options.AbortOnConnectFail = true;
        return ConnectionMultiplexer.ConnectAsync(options);
    }

    /// <summary>
    /// Converts <paramref name="url"/> (a <c>redis://</c> URL or a StackExchange.Redis configuration
    /// string) to <see cref="ConfigurationOptions"/>.
    /// </summary>
    public static ConfigurationOptions ToConfigurationOptions(string url)
    {
        if (!url.StartsWith("redis://", StringComparison.OrdinalIgnoreCase))
        {
            return ConfigurationOptions.Parse(url);
        }

        var uri = new Uri(url);
        var options = new ConfigurationOptions();
        options.EndPoints.Add(uri.Host, uri.IsDefaultPort ? 6379 : uri.Port);

        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            // redis://user:password@host or redis://:password@host
            var separator = uri.UserInfo.IndexOf(':');
            var user = separator < 0 ? string.Empty : Uri.UnescapeDataString(uri.UserInfo[..separator]);
            var password = Uri.UnescapeDataString(separator < 0 ? uri.UserInfo : uri.UserInfo[(separator + 1)..]);
            if (user.Length > 0)
            {
                options.User = user;
            }

            options.Password = password;
        }

        var database = uri.AbsolutePath.Trim('/');
        if (database.Length > 0)
        {
            options.DefaultDatabase = int.Parse(database, System.Globalization.CultureInfo.InvariantCulture);
        }

        return options;
    }
}
