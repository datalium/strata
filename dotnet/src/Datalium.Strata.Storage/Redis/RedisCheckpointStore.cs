using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Datalium.Strata.Interruption;
using StackExchange.Redis;

namespace Datalium.Strata.Storage.Redis;

/// <summary>
/// Redis <see cref="ICheckpointStore"/> adapter -- the other half of the "Redis/PostgreSQL" tier from
/// <c>design/03-Architecture.md</c>, and deliberately <i>not</i> SQL-shaped: no table, no
/// <c>sequence</c> column, no <c>MAX(sequence) + 1</c>. Async on StackExchange.Redis
/// (<c>design/19-Async-Queryable-Store.md</c>); the synchronous members wrap the async ones.
///
/// Each thread is one Redis <c>LIST</c> at <c>{prefix}:checkpoints:{thread_id}</c>, one JSON checkpoint
/// (the wire shape of <c>schema/checkpoint.schema.json</c>) per save, and <see cref="LoadAsync"/> is
/// <c>LINDEX key -1</c>, the newest. Listing threads reads the index sorted sets <c>{prefix}:threads</c>
/// and <c>{prefix}:threads:{state}</c>, scored by the latest checkpoint's <c>updated_at</c> digits
/// (<c>2026-10-02T09:15:00Z</c> scores <c>20261002091500</c>), and a due listing the set
/// <c>{prefix}:threads:waking</c> of waiting threads with a <c>wake_at</c>, scored the same way
/// (<c>design/29-Event-Waits-And-Timers.md</c>). A save is one Lua script
/// (<see cref="SaveScript"/>) that appends and updates the index atomically, so any number of
/// concurrent writers -- from either runtime -- is safe. It is the same keys, JSON and scripts
/// <c>strata-memory</c>'s <c>RedisCheckpointStore</c> uses, so either runtime can resume and list the
/// other's threads.
///
/// Every writer to one prefix must maintain the index: a runtime from before SG-11 only
/// <c>RPUSH</c>es, so its threads don't appear in listings until <see cref="RebuildIndexAsync"/> runs. A
/// prefix is indexed once, automatically, the first time a store is created for it.
///
/// Not built: expiry/eviction (a list grows until you delete it -- Redis is not told a thread is
/// finished), Redis Cluster/Sentinel (one save's keys hash to different slots), and TLS beyond what
/// StackExchange.Redis offers through your own <see cref="IDatabase"/>. Whether a checkpoint survives a
/// Redis restart is up to Redis's RDB/AOF settings, not this adapter.
/// </summary>
public sealed class RedisCheckpointStore : ICheckpointStore, ICheckpointQuery, ICheckpointRetention, IDisposable
{
    /// <summary>
    /// Appends a checkpoint and moves its thread to the right index sets, in one atomic step.
    /// <c>KEYS</c>: the thread's list, <c>{prefix}:threads</c>, <c>{prefix}:threads:waking</c>, then the state
    /// sets in <see cref="CheckpointThreadStates.All"/> order, then the thread's blob hash
    /// (<c>{prefix}:blobs:{thread_id}</c>). <c>ARGV</c>: the checkpoint JSON, the thread id, its score, its state's
    /// position (1 to the number of state sets), and <c>1</c> if it is waiting with a <c>wake_at</c>
    /// (<c>design/29-Event-Waits-And-Timers.md</c>), else <c>0</c>. Identical to <c>strata-memory</c>'s
    /// <c>SAVE_SCRIPT</c>.
    ///
    /// <c>ARGV[6]</c> is the finished-thread ttl in milliseconds (<c>design/34-Storage-Hardening.md</c>,
    /// "Retention"; 0 or absent: none): a save that leaves the thread completed or ended (positions 5 and 6)
    /// gives the list and the blob hash that long to live, and any other save clears their expiry.
    ///
    /// <c>ARGV[7]</c>, when not empty, is the sequence of the row the checkpoint relies on
    /// (<c>design/41-Checkpoint-Efficiency.md</c>, "Deltas" and "Offload"), and <c>ARGV[8]</c> the SHA-1 of that
    /// row's element: when the thread's latest element isn't that row any more the script writes nothing and
    /// returns -2, and the store saves the checkpoint whole. From <c>ARGV[9]</c> on, pairs of a blob's hash and
    /// content, written to the blob hash (<c>HSETNX</c>) once the checks pass.
    /// </summary>
    public const string SaveScript = """
        local blobs = KEYS[#KEYS]
        if ARGV[7] and ARGV[7] ~= '' and (redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[7])
          or redis.sha1hex(redis.call('LINDEX', KEYS[1], -1) or '') ~= ARGV[8]) then return -2 end
        for i = 9, #ARGV, 2 do redis.call('HSETNX', blobs, ARGV[i], ARGV[i + 1]) end
        redis.call('RPUSH', KEYS[1], ARGV[1])
        redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
        if ARGV[5] == '1' then
          redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
        else
          redis.call('ZREM', KEYS[3], ARGV[2])
        end
        local position = tonumber(ARGV[4])
        for i = 4, #KEYS - 1 do
          if i - 3 == position then
            redis.call('ZADD', KEYS[i], ARGV[3], ARGV[2])
          else
            redis.call('ZREM', KEYS[i], ARGV[2])
          end
        end
        if position == 5 or position == 6 then
          local ttl = tonumber(ARGV[6] or '') or 0
          if ttl > 0 then
            redis.call('PEXPIRE', KEYS[1], ttl)
            redis.call('PEXPIRE', blobs, ttl)
          end
        else
          redis.call('PERSIST', KEYS[1])
          redis.call('PERSIST', blobs)
        end
        return redis.call('LLEN', KEYS[1])
        """;

    /// <summary>
    /// <see cref="SaveScript"/>, only if the thread's list still ends at the sequence <c>ARGV[6]</c>
    /// (<c>design/28-Ending-A-Thread.md</c>, "Races"): returns 1 if it saved, 0 if anything was appended since.
    /// <c>KEYS</c> and the first five <c>ARGV</c> as for <see cref="SaveScript"/>; the ttl is <c>ARGV[7]</c>, the
    /// row relied on <c>ARGV[8]</c> and <c>ARGV[9]</c> (-2 when it moved), the blobs from <c>ARGV[10]</c> on.
    /// Identical to <c>strata-memory</c>'s <c>SAVE_IF_LATEST_SCRIPT</c>.
    /// </summary>
    public const string SaveIfLatestScript = """
        local blobs = KEYS[#KEYS]
        if redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[6]) then return 0 end
        if ARGV[8] and ARGV[8] ~= '' and (redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[8])
          or redis.sha1hex(redis.call('LINDEX', KEYS[1], -1) or '') ~= ARGV[9]) then return -2 end
        for i = 10, #ARGV, 2 do redis.call('HSETNX', blobs, ARGV[i], ARGV[i + 1]) end
        redis.call('RPUSH', KEYS[1], ARGV[1])
        redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
        if ARGV[5] == '1' then
          redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
        else
          redis.call('ZREM', KEYS[3], ARGV[2])
        end
        local position = tonumber(ARGV[4])
        for i = 4, #KEYS - 1 do
          if i - 3 == position then
            redis.call('ZADD', KEYS[i], ARGV[3], ARGV[2])
          else
            redis.call('ZREM', KEYS[i], ARGV[2])
          end
        end
        if position == 5 or position == 6 then
          local ttl = tonumber(ARGV[7] or '') or 0
          if ttl > 0 then
            redis.call('PEXPIRE', KEYS[1], ttl)
            redis.call('PEXPIRE', blobs, ttl)
          end
        else
          redis.call('PERSIST', KEYS[1])
          redis.call('PERSIST', blobs)
        end
        return 1
        """;

    /// <summary>
    /// <see cref="SaveIfLatestScript"/>, only if the thread's lease token is still <c>ARGV[7]</c>
    /// (<c>design/32-Thread-Leases.md</c>, "Fencing"): returns -1, having written nothing, when another owner
    /// acquired the thread since; else the list's length if it saved, 0 if anything was appended since the
    /// sequence <c>ARGV[6]</c> (empty for an unconditional save). <c>KEYS</c> as for <see cref="SaveScript"/>,
    /// then the thread's token counter last; the first five <c>ARGV</c> as for <see cref="SaveScript"/>; the ttl
    /// is <c>ARGV[8]</c>, and the token counter lives and dies with the list; the row relied on is <c>ARGV[9]</c>
    /// and <c>ARGV[10]</c> (-2 when it moved), the blobs from <c>ARGV[11]</c> on. Identical to
    /// <c>strata-memory</c>'s <c>SAVE_UNDER_LEASE_SCRIPT</c>.
    /// </summary>
    public const string SaveUnderLeaseScript = """
        local blobs = KEYS[#KEYS - 1]
        if redis.call('GET', KEYS[#KEYS]) ~= ARGV[7] then return -1 end
        if ARGV[6] ~= '' and redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[6]) then return 0 end
        if ARGV[9] and ARGV[9] ~= '' and (redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[9])
          or redis.sha1hex(redis.call('LINDEX', KEYS[1], -1) or '') ~= ARGV[10]) then return -2 end
        for i = 11, #ARGV, 2 do redis.call('HSETNX', blobs, ARGV[i], ARGV[i + 1]) end
        redis.call('RPUSH', KEYS[1], ARGV[1])
        redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
        if ARGV[5] == '1' then
          redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
        else
          redis.call('ZREM', KEYS[3], ARGV[2])
        end
        local position = tonumber(ARGV[4])
        for i = 4, #KEYS - 2 do
          if i - 3 == position then
            redis.call('ZADD', KEYS[i], ARGV[3], ARGV[2])
          else
            redis.call('ZREM', KEYS[i], ARGV[2])
          end
        end
        if position == 5 or position == 6 then
          local ttl = tonumber(ARGV[8] or '') or 0
          if ttl > 0 then
            redis.call('PEXPIRE', KEYS[1], ttl)
            redis.call('PEXPIRE', blobs, ttl)
            redis.call('PEXPIRE', KEYS[#KEYS], ttl)
          end
        else
          redis.call('PERSIST', KEYS[1])
          redis.call('PERSIST', blobs)
          redis.call('PERSIST', KEYS[#KEYS])
        end
        return redis.call('LLEN', KEYS[1])
        """;

    /// <summary>
    /// Takes a thread's lease unless it is held (<c>design/32-Thread-Leases.md</c>). <c>KEYS</c>: the thread's
    /// lease hash (<c>{prefix}:leases:{thread_id}</c>, which expires with the lease) and its token counter
    /// (<c>{prefix}:lease-tokens:{thread_id}</c>, which never does). <c>ARGV</c>: the owner and the ttl in
    /// milliseconds. Returns <c>{1, token, owner, expires_at}</c> for the new lease, or <c>{0, …}</c> with the
    /// lease that holds the thread; <c>expires_at</c> is Unix milliseconds on Redis's clock. Identical to
    /// <c>strata-memory</c>'s <c>ACQUIRE_LEASE_SCRIPT</c>.
    /// </summary>
    public const string AcquireLeaseScript = """
        local time = redis.call('TIME')
        local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
        if redis.call('EXISTS', KEYS[1]) == 1 then
          local held = redis.call('HMGET', KEYS[1], 'owner', 'token')
          return {0, tonumber(held[2]), held[1], now + redis.call('PTTL', KEYS[1])}
        end
        local token = redis.call('INCR', KEYS[2])
        redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'token', token)
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        return {1, token, ARGV[1], now + tonumber(ARGV[2])}
        """;

    /// <summary>
    /// Extends a lease whose token (<c>ARGV[3]</c>) is still the thread's latest, restoring the hash if it had
    /// expired: returns the new <c>expires_at</c>, or 0 when another owner acquired the thread. <c>KEYS</c> as
    /// for <see cref="AcquireLeaseScript"/>; <c>ARGV</c>: the owner, the ttl in milliseconds, the token.
    /// Identical to <c>strata-memory</c>'s <c>RENEW_LEASE_SCRIPT</c>.
    /// </summary>
    public const string RenewLeaseScript = """
        if redis.call('GET', KEYS[2]) ~= ARGV[3] then return 0 end
        redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'token', ARGV[3])
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        local time = redis.call('TIME')
        return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) + tonumber(ARGV[2])
        """;

    /// <summary>
    /// Gives up a lease whose token (<c>ARGV[1]</c>) is still the thread's latest: 1 if it did, 0 when another
    /// owner acquired the thread. The token counter stays. <c>KEYS</c> as for <see cref="AcquireLeaseScript"/>.
    /// Identical to <c>strata-memory</c>'s <c>RELEASE_LEASE_SCRIPT</c>.
    /// </summary>
    public const string ReleaseLeaseScript = """
        if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
        redis.call('DEL', KEYS[1])
        return 1
        """;

    /// <summary>
    /// The lease a thread is held under: <c>{token, owner, expires_at}</c>, or nil when nobody holds it.
    /// <c>KEYS</c>: the thread's lease hash. Identical to <c>strata-memory</c>'s <c>GET_LEASE_SCRIPT</c>.
    /// </summary>
    public const string GetLeaseScript = """
        local held = redis.call('HMGET', KEYS[1], 'owner', 'token')
        if not held[1] then return false end
        local time = redis.call('TIME')
        return {tonumber(held[2]), held[1],
          tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) + redis.call('PTTL', KEYS[1])}
        """;

    /// <summary>
    /// Indexes one thread from its own latest checkpoint, read inside the script so a save racing the
    /// index pass can't be overwritten by an older state. A thread whose list is gone (it expired, or was
    /// deleted) is taken out of every set. <c>KEYS</c> as for <see cref="SaveScript"/>;
    /// <c>ARGV</c>: the thread id. Identical to <c>strata-memory</c>'s <c>INDEX_SCRIPT</c>.
    /// </summary>
    public const string IndexScript = """
        local latest = redis.call('LINDEX', KEYS[1], -1)
        if not latest then
          for i = 2, #KEYS do redis.call('ZREM', KEYS[i], ARGV[1]) end
          return 0
        end
        local checkpoint = cjson.decode(latest)
        local state = checkpoint.status
        local updated = checkpoint.created_at
        if checkpoint.resumed_at ~= nil and checkpoint.resumed_at ~= cjson.null then
          state = 'resumed'
          updated = checkpoint.resumed_at
        end
        local level = checkpoint
        while level.nested ~= nil and level.nested ~= cjson.null do
          level = level.nested
        end
        local wait = level.wait
        local waking = state == 'interrupted' and wait ~= nil and wait ~= cjson.null
          and wait.wake_at ~= nil and wait.wake_at ~= cjson.null
        local score = string.gsub(updated, '%D', '')
        local states = {'interrupted', 'failed', 'cancelled', 'resumed', 'completed', 'ended', 'running'}
        redis.call('ZADD', KEYS[2], score, ARGV[1])
        if waking then
          redis.call('ZADD', KEYS[3], score, ARGV[1])
        else
          redis.call('ZREM', KEYS[3], ARGV[1])
        end
        for i = 4, #KEYS do
          if states[i - 3] == state then
            redis.call('ZADD', KEYS[i], score, ARGV[1])
          else
            redis.call('ZREM', KEYS[i], ARGV[1])
          end
        end
        return 1
        """;

    /// <summary>
    /// Deletes one thread if it is done with (<c>design/34-Storage-Hardening.md</c>, "Retention"), decided from
    /// its own latest checkpoint in the same atomic step: returns 1 if it deleted the thread's list, token
    /// counter, blob hash and index entries, 0 if the thread changed, is too new, isn't in a named state, or is
    /// held. <c>KEYS</c>: the thread's list and the index sets as for <see cref="IndexScript"/>, then the thread's
    /// lease hash, its token counter and its blob hash. <c>ARGV</c>: the thread id, the score its latest checkpoint
    /// must be below, then the states to purge. Identical to <c>strata-memory</c>'s <c>PURGE_SCRIPT</c>.
    /// </summary>
    public const string PurgeScript = """
        local sets = #KEYS - 3
        local latest = redis.call('LINDEX', KEYS[1], -1)
        if not latest then
          for i = 2, sets do redis.call('ZREM', KEYS[i], ARGV[1]) end
          return 0
        end
        local checkpoint = cjson.decode(latest)
        local state = checkpoint.status
        local updated = checkpoint.created_at
        if checkpoint.resumed_at ~= nil and checkpoint.resumed_at ~= cjson.null then
          state = 'resumed'
          updated = checkpoint.resumed_at
        end
        local score = string.gsub(updated, '%D', '')
        if tonumber(score) >= tonumber(ARGV[2]) then return 0 end
        local named = false
        for i = 3, #ARGV do
          if ARGV[i] == state then named = true end
        end
        if not named then return 0 end
        if redis.call('EXISTS', KEYS[#KEYS - 2]) == 1 then return 0 end
        redis.call('DEL', KEYS[1], KEYS[#KEYS - 1], KEYS[#KEYS])
        for i = 2, sets do redis.call('ZREM', KEYS[i], ARGV[1]) end
        return 1
        """;

    /// <summary>
    /// Up to <c>ARGV[3]</c> members of the sorted set <c>KEYS[1]</c> after a cursor, newest first, with
    /// their scores -- positioned atomically, so a thread that moves while the page is read can't shift
    /// the ones after it. <c>ARGV[1]</c>/<c>ARGV[2]</c> are the cursor's score and thread id (empty for
    /// none). Identical to <c>strata-memory</c>'s <c>CANDIDATES_SCRIPT</c>.
    /// </summary>
    public const string CandidatesScript = """
        local start = 0
        if ARGV[1] ~= '' then
          local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
          if score and tonumber(score) == tonumber(ARGV[1]) then
            start = redis.call('ZREVRANK', KEYS[1], ARGV[2]) + 1
          else
            start = redis.call('ZCOUNT', KEYS[1], '(' .. ARGV[1], '+inf')
            local ties = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[1], 'BYSCORE')
            for _, member in ipairs(ties) do
              if member > ARGV[2] then start = start + 1 end
            end
          end
        end
        return redis.call('ZRANGE', KEYS[1], start, start + tonumber(ARGV[3]) - 1, 'REV', 'WITHSCORES')
        """;

    /// <summary>How many candidates one listing call examines at most, per page slot.</summary>
    private const int ExaminedPerSlot = 10;

    private readonly IDatabase _database;
    private readonly string _prefix;
    private readonly RedisStoreOptions _options;
    private readonly StoreRetrier _retry;
    private readonly long _finishedTtl;
    private readonly SemaphoreSlim _indexing = new(1, 1);
    private readonly ConnectionMultiplexer? _ownedConnection;
    private readonly CheckpointWriter? _writer;

    /// <summary>Where offloaded values go when not in this Redis.</summary>
    private readonly ICheckpointBlobStore? _blobs;
    private volatile bool _indexed;

    /// <param name="database">An existing database handle, e.g. from your application's shared multiplexer.</param>
    /// <param name="prefix">Key namespace; keys are <c>{prefix}:checkpoints:…</c>.</param>
    public RedisCheckpointStore(IDatabase database, string prefix = RedisConnection.DefaultPrefix)
        : this(database, new RedisStoreOptions { Prefix = prefix })
    {
    }

    /// <summary>
    /// A store over <paramref name="database"/>. Nothing is sent until the store is used
    /// (<c>design/34-Storage-Hardening.md</c>): the first call indexes a prefix that a runtime from before the
    /// index wrote to.
    /// </summary>
    public RedisCheckpointStore(IDatabase database, RedisStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(options);
        if (options.FinishedThreadTtl is { } ttl)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(ttl, TimeSpan.FromMilliseconds(1), nameof(options));
        }

        _database = database;
        _options = options;
        _prefix = options.Prefix;
        _finishedTtl = (long)(options.FinishedThreadTtl?.TotalMilliseconds ?? 0);
        _retry = new StoreRetrier(options.Retry, options.TimeProvider, "redis", RedisFaults.IsTransient);
        _writer = CheckpointWriter.For(options.Deltas, options.Offload);
        _blobs = options.Offload?.Blobs;
    }

    private RedisCheckpointStore(ConnectionMultiplexer connection, RedisStoreOptions options)
        : this(connection.GetDatabase(), options)
    {
        _ownedConnection = connection;
    }

    /// <summary>Connects to <paramref name="url"/> (see <see cref="RedisConnection.Connect"/>) and owns the connection.</summary>
    public static RedisCheckpointStore Connect(string url, string prefix = RedisConnection.DefaultPrefix) =>
        new(RedisConnection.Connect(url), new RedisStoreOptions { Prefix = prefix });

    /// <summary><see cref="Connect"/> without blocking a thread on the connection, and with <paramref name="options"/>.</summary>
    public static async Task<RedisCheckpointStore> ConnectAsync(string url, RedisStoreOptions? options = null) =>
        new(await RedisConnection.ConnectAsync(url).ConfigureAwait(false), options ?? new RedisStoreOptions());

    /// <summary>
    /// Indexes the prefix the first time the store is used, if no store has yet
    /// (<c>design/19-Async-Queryable-Store.md</c>, "Migration"). A failure is the call's, and the next call
    /// tries again.
    /// </summary>
    private async ValueTask ReadyAsync(CancellationToken cancellationToken)
    {
        if (_indexed)
        {
            return;
        }

        await _indexing.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_indexed)
            {
                return;
            }

            await _retry.RunAsync(
                    "migrate",
                    async (_, token) =>
                    {
                        if (!await _database.KeyExistsAsync(IndexedKey).ConfigureAwait(false))
                        {
                            await IndexAllAsync(token).ConfigureAwait(false);
                            await _database.StringSetAsync(IndexedKey, "1").ConfigureAwait(false);
                        }
                    },
                    cancellationToken)
                .ConfigureAwait(false);
            _indexed = true;
        }
        finally
        {
            _indexing.Release();
        }
    }

    /// <summary>Makes sure the prefix is indexed, then makes <paramref name="call"/> under the retry policy.</summary>
    private async ValueTask<T> RunAsync<T>(
        string operation, Func<Task<T>> call, CancellationToken cancellationToken, Func<Exception, bool>? unless = null)
    {
        await ReadyAsync(cancellationToken).ConfigureAwait(false);
        return await _retry.RunAsync(operation, (_, _) => new ValueTask<T>(call()), cancellationToken, unless).ConfigureAwait(false);
    }

    private RedisKey Key(string threadId) => $"{_prefix}:checkpoints:{threadId}";

    private RedisKey ThreadsKey => $"{_prefix}:threads";

    private RedisKey StateKey(CheckpointThreadState state) => $"{_prefix}:threads:{state.Name()}";

    private RedisKey IndexedKey => $"{_prefix}:threads:indexed";

    /// <summary>
    /// The waiting threads with a <c>wake_at</c>, scored by <c>updated_at</c> like every other index set, so a
    /// due listing keeps the listing order and cursor (<c>design/29-Event-Waits-And-Timers.md</c>).
    /// </summary>
    private RedisKey WakingKey => $"{_prefix}:threads:waking";

    /// <summary>A held thread's lease, <c>owner</c> and <c>token</c>, expiring with it (<c>design/32-Thread-Leases.md</c>).</summary>
    private RedisKey LeaseKey(string threadId) => $"{_prefix}:leases:{threadId}";

    /// <summary>A thread's latest lease token: a counter that outlives every lease.</summary>
    private RedisKey LeaseTokenKey(string threadId) => $"{_prefix}:lease-tokens:{threadId}";

    private RedisKey[] LeaseKeys(string threadId) => [LeaseKey(threadId), LeaseTokenKey(threadId)];

    /// <summary>
    /// A thread's offloaded values, hash to content (<c>design/41-Checkpoint-Efficiency.md</c>, "Offload"), kept,
    /// expired and deleted with its list.
    /// </summary>
    private RedisKey BlobKey(string threadId) => $"{_prefix}:blobs:{threadId}";

    /// <summary>The token a row is checked by when a save relies on it: the SHA-1 of its element, as Redis's <c>sha1hex</c> gives it.</summary>
    private static string Token(string element) => Convert.ToHexStringLower(SHA1.HashData(Encoding.UTF8.GetBytes(element)));

    /// <summary>The script <c>KEYS</c> for a thread: its list, the all-threads set, the waking set and the state sets.</summary>
    private RedisKey[] IndexKeys(string threadId) =>
        [Key(threadId), ThreadsKey, WakingKey, .. CheckpointThreadStates.All.Select(StateKey)];

    /// <summary>The index score of a stored timestamp: its digits, as a number.</summary>
    private static long Score(string updatedAt) =>
        long.Parse(
            new string([.. CheckpointTimestamps.Text(CheckpointTimestamps.Parse(updatedAt)).Where(char.IsAsciiDigit)]),
            CultureInfo.InvariantCulture);

    /// <summary>The inverse of <see cref="Score"/>, for cursors.</summary>
    private static string ScoreText(long score)
    {
        var d = score.ToString("D14", CultureInfo.InvariantCulture);
        return $"{d[..4]}-{d[4..6]}-{d[6..8]}T{d[8..10]}:{d[10..12]}:{d[12..14]}Z";
    }

    /// <summary>Synchronous <see cref="SaveAsync"/>.</summary>
    public void Save(Checkpoint checkpoint) => SaveAsync(checkpoint).AsTask().GetAwaiter().GetResult();

    /// <summary>Synchronous <see cref="LoadAsync"/>.</summary>
    public Checkpoint? Load(string threadId) => LoadAsync(threadId).AsTask().GetAwaiter().GetResult();

    public async ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default) =>
        await SaveOnceAsync(checkpoint, expectedSequence: null, lease: null, cancellationToken).ConfigureAwait(false);

    /// <summary>Appends with <see cref="SaveIfLatestScript"/> when there is a sequence to compare against.</summary>
    public async ValueTask<bool> SaveIfLatestAsync(
        Checkpoint checkpoint, long? expectedSequence, CancellationToken cancellationToken = default)
    {
        if (expectedSequence is not { } expected)
        {
            await SaveAsync(checkpoint, cancellationToken).ConfigureAwait(false);
            return true;
        }

        return await SaveOnceAsync(checkpoint, expected, lease: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Appends with <see cref="SaveUnderLeaseScript"/>, which checks the lease's token in the same atomic step.</summary>
    public async ValueTask<bool> SaveUnderLeaseAsync(
        Checkpoint checkpoint, ThreadLease lease, long? expectedSequence = null, CancellationToken cancellationToken = default)
    {
        ThreadLeases.CheckThread(checkpoint, lease);
        return await SaveOnceAsync(checkpoint, expectedSequence, lease, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>What a save script said, as a store reads it.</summary>
    private enum Reply
    {
        Saved,
        Lost,
        LeaseLost,

        /// <summary>A save whose base isn't the thread's latest any more: nothing written.</summary>
        Rebase,
    }

    /// <summary>
    /// Runs the save script for <paramref name="save"/>: <see cref="SaveUnderLeaseScript"/> under a
    /// <paramref name="lease"/>, <see cref="SaveIfLatestScript"/> with an <paramref name="expectedSequence"/>, else
    /// <see cref="SaveScript"/>, with the blobs it writes when they are kept in this Redis. Returns what it did and,
    /// when it saved, the sequence it saved at, the bytes of its element and the element's token.
    /// </summary>
    private async Task<(Reply Reply, long Sequence, long Bytes, string Token)> SaveScriptAsync(
        EncodedSave save, long? expectedSequence, ThreadLease? lease)
    {
        var threadId = save.Checkpoint.ThreadId;
        RedisValue baseSequence = save.Base is { } basis ? basis.Sequence : RedisValue.EmptyString;
        RedisValue baseToken = save.Base?.Token ?? string.Empty;
        var arguments = SaveArguments(save);
        var element = (string)arguments[0]!;
        var bytes = (long)Encoding.UTF8.GetByteCount(element);
        RedisValue[] blobs = _blobs is null
            ? [.. save.Blobs.SelectMany(blob => (RedisValue[])[blob.Hash, blob.Content])]
            : [];
        if (lease is not null)
        {
            var saved = (long)await _database.ScriptEvaluateAsync(
                SaveUnderLeaseScript,
                [.. IndexKeys(threadId), BlobKey(threadId), LeaseTokenKey(threadId)],
                [
                    .. arguments,
                    expectedSequence is { } expected ? expected : RedisValue.EmptyString,
                    lease.Token,
                    _finishedTtl,
                    baseSequence,
                    baseToken,
                    .. blobs,
                ]).ConfigureAwait(false);
            return saved switch
            {
                -2 => (Reply.Rebase, -1, 0, string.Empty),
                -1 => (Reply.LeaseLost, -1, 0, string.Empty),
                0 => (Reply.Lost, -1, 0, string.Empty),
                _ => (Reply.Saved, saved - 1, bytes, Token(element)),
            };
        }

        if (expectedSequence is { } after)
        {
            var saved = (long)await _database.ScriptEvaluateAsync(
                SaveIfLatestScript,
                [.. IndexKeys(threadId), BlobKey(threadId)],
                [.. arguments, after, _finishedTtl, baseSequence, baseToken, .. blobs]).ConfigureAwait(false);
            return saved switch
            {
                -2 => (Reply.Rebase, -1, 0, string.Empty),
                1 => (Reply.Saved, after + 1, bytes, Token(element)),
                _ => (Reply.Lost, -1, 0, string.Empty),
            };
        }

        var length = (long)await _database.ScriptEvaluateAsync(
            SaveScript,
            [.. IndexKeys(threadId), BlobKey(threadId)],
            [.. arguments, _finishedTtl, baseSequence, baseToken, .. blobs]).ConfigureAwait(false);
        return length == -2 ? (Reply.Rebase, -1, 0, string.Empty) : (Reply.Saved, length - 1, bytes, Token(element));
    }

    /// <summary>
    /// Makes a save under the retry policy so that it happens once
    /// (<c>design/34-Storage-Hardening.md</c>, "A save that may have happened"). A save is one script, so an
    /// attempt that failed after it was sent may have run: the next attempt looks at the list first, where
    /// the checkpoint would be, and only saves again if it isn't there. A conditional save whose place was
    /// taken by another checkpoint lost its race.
    /// </summary>
    private async ValueTask<bool> SaveOnceAsync(
        Checkpoint checkpoint, long? expectedSequence, ThreadLease? lease, CancellationToken cancellationToken)
    {
        await ReadyAsync(cancellationToken).ConfigureAwait(false);
        var save = _writer?.Prepare(checkpoint) ?? EncodedSave.Plain(checkpoint);
        if (_blobs is { } elsewhere)
        {
            // Blobs in another store go before the row that refers to them.
            await OffloadedBlobs.PutAsync(elsewhere, checkpoint.ThreadId, save.Blobs, cancellationToken).ConfigureAwait(false);
        }

        var mayBeWritten = false;
        return await _retry.RunAsync(
                "save",
                async (_, _) =>
                {
                    if (mayBeWritten)
                    {
                        if (_options.Fault is { } recheck)
                        {
                            await recheck("save:recheck").ConfigureAwait(false);
                        }

                        // The store no longer knows which row is the thread's latest, whatever it finds.
                        var stored = await RowAsync(checkpoint.ThreadId, expectedSequence + 1, cancellationToken).ConfigureAwait(false);
                        _writer?.Forget(checkpoint.ThreadId);
                        if (stored is { } found)
                        {
                            if (StoredCheckpoints.Same(checkpoint, found.Row.Checkpoint))
                            {
                                return true;
                            }

                            if (expectedSequence is not null)
                            {
                                return false;
                            }
                        }

                        mayBeWritten = false;
                    }

                    try
                    {
                        if (_options.Fault is { } before)
                        {
                            await before("save:before").ConfigureAwait(false);
                        }

                        var (reply, sequence, bytes, token) = await SaveScriptAsync(save, expectedSequence, lease).ConfigureAwait(false);
                        if (reply == Reply.Rebase)
                        {
                            save = save.Whole();
                            if (_blobs is { } blobs)
                            {
                                await OffloadedBlobs.PutAsync(blobs, checkpoint.ThreadId, save.Blobs, cancellationToken).ConfigureAwait(false);
                            }

                            (reply, sequence, bytes, token) = await SaveScriptAsync(save, expectedSequence, lease).ConfigureAwait(false);
                        }

                        if (_options.Fault is { } after)
                        {
                            await after("save:after").ConfigureAwait(false);
                        }

                        switch (reply)
                        {
                            case Reply.Saved:
                                StorageTelemetry.Stored("redis", save, bytes);
                                _writer?.Saved(save, sequence, token);
                                return true;
                            case Reply.LeaseLost:
                                throw new LeaseLostException(lease!);
                            default:
                                _writer?.Forget(checkpoint.ThreadId);
                                return false;
                        }
                    }
                    catch (Exception ex) when (_retry.IsTransient(ex) && !RedisFaults.NeverSent(ex))
                    {
                        mayBeWritten = true;
                        throw;
                    }
                },
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<LeaseAttempt> TryAcquireLeaseAsync(
        string threadId, string owner, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThreadLeases.Check(owner, ttl);

        // An acquisition that may have run is not made again: the store can't tell its own lease from another
        // process's with the same owner name. Only a script that never reached the server is repeated.
        var reply = (RedisResult[])(await RunAsync(
            "lease",
            () => _database.ScriptEvaluateAsync(AcquireLeaseScript, LeaseKeys(threadId), [owner, ThreadLeases.Milliseconds(ttl)]),
            cancellationToken,
            unless: failure => !RedisFaults.NeverSent(failure)).ConfigureAwait(false))!;
        var lease = new ThreadLease(
            threadId, (string)reply[2]!, (long)reply[1], DateTimeOffset.FromUnixTimeMilliseconds((long)reply[3]));
        return (int)reply[0] == 1 ? new LeaseAttempt(lease, null) : new LeaseAttempt(null, lease);
    }

    public async ValueTask<ThreadLease?> RenewLeaseAsync(
        ThreadLease lease, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ThreadLeases.CheckTtl(ttl);
        var expiresAt = (long)await RunAsync(
            "lease",
            () => _database.ScriptEvaluateAsync(
                RenewLeaseScript, LeaseKeys(lease.ThreadId), [lease.Owner, ThreadLeases.Milliseconds(ttl), lease.Token]),
            cancellationToken).ConfigureAwait(false);
        return expiresAt == 0 ? null : lease with { ExpiresAt = DateTimeOffset.FromUnixTimeMilliseconds(expiresAt) };
    }

    public async ValueTask<bool> ReleaseLeaseAsync(ThreadLease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        return (int)await RunAsync(
            "lease",
            () => _database.ScriptEvaluateAsync(ReleaseLeaseScript, LeaseKeys(lease.ThreadId), [lease.Token]),
            cancellationToken).ConfigureAwait(false) == 1;
    }

    public async ValueTask<ThreadLease?> GetLeaseAsync(string threadId, CancellationToken cancellationToken = default)
    {
        var reply = await RunAsync(
            "lease", () => _database.ScriptEvaluateAsync(GetLeaseScript, [LeaseKey(threadId)]), cancellationToken)
            .ConfigureAwait(false);
        if (reply.IsNull)
        {
            return null;
        }

        var held = (RedisResult[])reply!;
        return new ThreadLease(
            threadId, (string)held[1]!, (long)held[0], DateTimeOffset.FromUnixTimeMilliseconds((long)held[2]));
    }

    /// <summary>The first five <c>ARGV</c> of <see cref="SaveScript"/> for <paramref name="save"/>: its stored JSON, then its index entries.</summary>
    private static RedisValue[] SaveArguments(EncodedSave save)
    {
        var arguments = SaveArguments(save.Checkpoint);
        arguments[0] = save.Stored.ToWireJson().ToJsonString();
        return arguments;
    }

    /// <summary>The first five <c>ARGV</c> of <see cref="SaveScript"/> for <paramref name="checkpoint"/>.</summary>
    private static RedisValue[] SaveArguments(Checkpoint checkpoint) =>
    [
        checkpoint.ToWireJson().ToJsonString(),
        checkpoint.ThreadId,
        Score(CheckpointTimestamps.UpdatedAt(checkpoint)),
        CheckpointThreadStates.All.ToList().IndexOf(CheckpointThreadStates.Of(checkpoint)) + 1,
        CheckpointThreadStates.Of(checkpoint) == CheckpointThreadState.Interrupted
            && checkpoint.InnermostWait()?.WakeAt is not null
            ? "1"
            : "0",
    ];

    public async ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default) =>
        (await LoadLatestAsync(threadId, cancellationToken).ConfigureAwait(false))?.Checkpoint;

    /// <summary>
    /// The newest element and the list's length, read together; the sequence is its position. A delta is read
    /// with the elements before it that it needs, in one more range read; the store then knows the thread's latest.
    /// </summary>
    public async ValueTask<LatestCheckpoint?> LoadLatestAsync(string threadId, CancellationToken cancellationToken = default)
    {
        var latest = await RunAsync("load", () => RowAsync(threadId, null, cancellationToken), cancellationToken).ConfigureAwait(false);
        if (latest is not { } row)
        {
            return null;
        }

        _writer?.Loaded(row.Sequence, row.Depth, row.Row, row.Token);
        return new LatestCheckpoint(row.Row.Checkpoint, row.Sequence);
    }

    private static Checkpoint Parse(RedisValue element) => Checkpoint.FromWireJson(JsonNode.Parse((string)element!)!);

    /// <summary>
    /// The thread's element at <paramref name="sequence"/> (the newest when <see langword="null"/>), decoded, its
    /// offloaded values read, with its sequence, how far it is from a whole row and its token; one attempt.
    /// </summary>
    private async Task<(long Sequence, DecodedRow Row, int Depth, string Token)?> RowAsync(
        string threadId, long? sequence, CancellationToken cancellationToken)
    {
        RedisValue element;
        long at;
        if (sequence is { } index)
        {
            element = await _database.ListGetByIndexAsync(Key(threadId), index).ConfigureAwait(false);
            at = index;
        }
        else
        {
            var transaction = _database.CreateTransaction();
            var latest = transaction.ListGetByIndexAsync(Key(threadId), -1);
            var length = transaction.ListLengthAsync(Key(threadId));
            await transaction.ExecuteAsync().ConfigureAwait(false);
            element = await latest.ConfigureAwait(false);
            at = await length.ConfigureAwait(false) - 1;
        }

        if (element.IsNull)
        {
            return null;
        }

        var stored = Parse(element);
        var depth = CheckpointEncoding.DepthOf(stored, at);
        List<(long, Checkpoint)> rows = depth == 0 ? [] : await RangeAsync(threadId, at - depth, at - 1).ConfigureAwait(false);
        rows.Add((at, stored));
        var row = (await CheckpointEncoding.DecodeAsync(rows, rows.Count - 1, ReaderFor(threadId), cancellationToken)
            .ConfigureAwait(false))[0];
        return (at, row, depth, Token((string)element!));
    }

    /// <summary>
    /// How <paramref name="threadId"/>'s offloaded values are read: from the blob store the options name, or from
    /// the thread's blob hash in this Redis, in one <c>HMGET</c>.
    /// </summary>
    private BlobReader ReaderFor(string threadId) => _blobs is { } blobs
        ? (hashes, cancellationToken) => OffloadedBlobs.ReadAsync(blobs, threadId, hashes, cancellationToken)
        : async (hashes, _) =>
        {
            var wanted = hashes.ToArray();
            var contents = await _database.HashGetAsync(BlobKey(threadId), [.. wanted.Select(hash => (RedisValue)hash)])
                .ConfigureAwait(false);
            var found = new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal);
            for (var i = 0; i < wanted.Length; i++)
            {
                if (!contents[i].IsNull)
                {
                    found[wanted[i]] = (byte[])contents[i]!;
                }
            }

            return found;
        };

    /// <summary>The thread's elements from <paramref name="from"/> to <paramref name="to"/>, as stored, with their sequences.</summary>
    private async Task<List<(long, Checkpoint)>> RangeAsync(string threadId, long from, long to)
    {
        var elements = await _database.ListRangeAsync(Key(threadId), from, to).ConfigureAwait(false);
        return [.. elements.Select((element, i) => (from + i, Parse(element)))];
    }

    /// <summary>
    /// Indexes every thread under this prefix from its latest checkpoint: repairs the index after writes
    /// from a runtime that predates it, and drops threads whose lists are gone. Safe to run while others
    /// write; returns how many threads it indexed.
    /// </summary>
    public async ValueTask<int> RebuildIndexAsync(CancellationToken cancellationToken = default)
    {
        await ReadyAsync(cancellationToken).ConfigureAwait(false);
        return await _retry.RunAsync("migrate", (_, token) => new ValueTask<int>(IndexAllAsync(token)), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>One pass of <see cref="RebuildIndexAsync"/>; indexing a thread twice is indexing it once.</summary>
    private async Task<int> IndexAllAsync(CancellationToken cancellationToken)
    {
        var listPrefix = $"{_prefix}:checkpoints:";
        var pattern = GlobEscape(listPrefix) + "*";
        var cursor = "0";
        var indexed = 0;
        do
        {
            var reply = (RedisResult[])(await _database.ExecuteAsync("SCAN", cursor, "MATCH", pattern, "COUNT", 500).ConfigureAwait(false))!;
            cursor = (string)reply[0]!;
            foreach (var key in (string[])reply[1]!)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var threadId = key[listPrefix.Length..];
                indexed += (int)await _database.ScriptEvaluateAsync(IndexScript, IndexKeys(threadId), [threadId])
                    .ConfigureAwait(false);
            }
        }
        while (cursor != "0");

        // A thread whose list expired is in no scan: look for it in the set of every thread.
        var listed = await _database.SortedSetRangeByRankAsync(ThreadsKey).ConfigureAwait(false);
        foreach (var chunk in listed.Chunk(500))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var present = await Task.WhenAll(chunk.Select(id => _database.KeyExistsAsync(Key((string)id!)))).ConfigureAwait(false);
            for (var i = 0; i < chunk.Length; i++)
            {
                if (!present[i])
                {
                    var threadId = (string)chunk[i]!;
                    await _database.ScriptEvaluateAsync(IndexScript, IndexKeys(threadId), [threadId]).ConfigureAwait(false);
                }
            }
        }

        return indexed;
    }

    private static string GlobEscape(string text) =>
        string.Concat(text.Select(c => c is '*' or '?' or '[' or ']' or '\\' ? $"\\{c}" : c.ToString()));

    /// <summary>Up to <paramref name="count"/> (score, thread id) candidates from <paramref name="set"/> after <paramref name="start"/>.</summary>
    private async Task<List<(long Score, string ThreadId)>> CandidatesAsync(RedisKey set, ThreadCursor? start, int count)
    {
        var flat = (string[])(await _database.ScriptEvaluateAsync(
            CandidatesScript,
            [set],
            [
                start is null ? string.Empty : Score(start.UpdatedAt).ToString(CultureInfo.InvariantCulture),
                start?.ThreadId ?? string.Empty,
                count,
            ]).ConfigureAwait(false))!;
        var candidates = new List<(long, string)>(flat.Length / 2);
        for (var i = 0; i < flat.Length; i += 2)
        {
            candidates.Add(((long)double.Parse(flat[i + 1], CultureInfo.InvariantCulture), flat[i]));
        }

        return candidates;
    }

    private static int Compare((long Score, string ThreadId) a, (long Score, string ThreadId) b) =>
        a.Score != b.Score ? a.Score.CompareTo(b.Score) : string.CompareOrdinal(a.ThreadId, b.ThreadId);

    /// <summary>
    /// One page of <paramref name="query"/> (<c>design/19-Async-Queryable-Store.md</c>, "Redis: index
    /// sorted sets"). Candidates come from the index sets in listing order; each one's latest checkpoint
    /// is then read and filtered. At most <see cref="ExaminedPerSlot"/> × <see cref="ThreadQuery.Limit"/>
    /// candidates are examined, so a filtered page may come back short with a cursor.
    /// </summary>
    public async ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default)
    {
        query.Validate();
        return await RunAsync("list", () => ListOnceAsync(query), cancellationToken).ConfigureAwait(false);
    }

    private async Task<ThreadPage> ListOnceAsync(ThreadQuery query)
    {
        var cursor = query.Validate();
        // Start at the cursor, or just below `UpdatedBefore` if that is lower.
        var start = cursor;
        if (query.UpdatedBeforeText is { } before)
        {
            var below = new ThreadCursor(before, string.Empty);
            if (start is null || (!below.Admits(start.UpdatedAt, start.ThreadId) && start != below))
            {
                start = below;
            }
        }

        var (candidates, more) = await CollectCandidatesAsync(query, start).ConfigureAwait(false);
        var examination = await ExamineAsync(candidates, query, start).ConfigureAwait(false);

        // A full page continues after its last thread if anything may follow; a short one only stops
        // early when the examination budget ran out.
        (long Score, string ThreadId)? position = null;
        if (examination.Threads.Count == query.Limit)
        {
            if (more || examination.Examined < candidates.Count)
            {
                position = examination.LastReturned;
            }
        }
        else if (more)
        {
            position = examination.LastExamined;
        }

        var next = position is { } p ? new ThreadCursor(ScoreText(p.Score), p.ThreadId).Encode() : null;
        return new ThreadPage(examination.Threads, next);
    }

    /// <summary>
    /// The candidates for a page, in listing order, and whether more may follow them. Each set that
    /// filled the budget may hold more below its last member, which could rank above candidates from
    /// the other sets, so only candidates at or above the highest such last member are kept.
    /// </summary>
    private async Task<(List<(long Score, string ThreadId)> Candidates, bool More)> CollectCandidatesAsync(
        ThreadQuery query, ThreadCursor? start)
    {
        // A due listing reads only the threads that can be due; every other filter is applied to each one.
        var sets = query.DueBy is not null ? [WakingKey]
            : query.States is { Count: > 0 } states ? states.Distinct().Order().Select(StateKey).ToList()
            : [ThreadsKey];
        var budget = query.Limit * ExaminedPerSlot;

        var fetchedAll = new List<(long Score, string ThreadId)>();
        (long Score, string ThreadId)? floor = null;
        foreach (var set in sets)
        {
            var fetched = await CandidatesAsync(set, start, budget).ConfigureAwait(false);
            if (fetched.Count == budget && (floor is null || Compare(fetched[^1], floor.Value) > 0))
            {
                floor = fetched[^1];
            }

            fetchedAll.AddRange(fetched);
        }

        var candidates = fetchedAll
            .DistinctBy(c => c.ThreadId)
            .OrderByDescending(c => c.Score)
            .ThenByDescending(c => c.ThreadId, StringComparer.Ordinal)
            .ToList();
        var more = floor is not null;
        if (floor is { } lowest)
        {
            candidates.RemoveAll(c => Compare(c, lowest) < 0);
        }

        if (query.UpdatedAfterText is { } after)
        {
            var afterScore = Score(after);
            var cut = candidates.FindIndex(c => c.Score <= afterScore);
            if (cut >= 0)
            {
                candidates.RemoveRange(cut, candidates.Count - cut);
                more = false;
            }
        }

        if (candidates.Count > budget)
        {
            candidates.RemoveRange(budget, candidates.Count - budget);
            more = true;
        }

        return (candidates, more);
    }

    private sealed record Examination(
        List<ThreadSummary> Threads,
        int Examined,
        (long Score, string ThreadId)? LastExamined,
        (long Score, string ThreadId)? LastReturned);

    /// <summary>
    /// Reads each candidate's latest checkpoint and log length together, a page-sized chunk at a time,
    /// and keeps the ones that match, until the page is full.
    /// </summary>
    private async Task<Examination> ExamineAsync(
        List<(long Score, string ThreadId)> candidates, ThreadQuery query, ThreadCursor? start)
    {
        var threads = new List<ThreadSummary>();
        (long Score, string ThreadId)? lastExamined = null;
        (long Score, string ThreadId)? lastReturned = null;
        var examined = 0;
        foreach (var chunk in candidates.Chunk(query.Limit))
        {
            var transaction = _database.CreateTransaction();
            var reads = chunk
                .Select(c => (Latest: transaction.ListGetByIndexAsync(Key(c.ThreadId), -1), Length: transaction.ListLengthAsync(Key(c.ThreadId))))
                .ToList();
            await transaction.ExecuteAsync().ConfigureAwait(false);

            for (var i = 0; i < chunk.Length && threads.Count < query.Limit; i++)
            {
                lastExamined = chunk[i];
                examined++;
                var latest = await reads[i].Latest.ConfigureAwait(false);
                if (latest.IsNull)
                {
                    // The thread's list is gone (it expired, or was deleted): take it out of the index.
                    await _database.ScriptEvaluateAsync(IndexScript, IndexKeys(chunk[i].ThreadId), [chunk[i].ThreadId])
                        .ConfigureAwait(false);
                    continue;
                }

                // A summary needs no state: an encoded element gives only the version it was saved with.
                var checkpoint = CheckpointEncoding.Listed(Parse(latest));
                // The index may lag a thread that moved since: its own latest checkpoint decides, so a
                // moved thread is never returned twice.
                var admitted = start is null || start.Admits(CheckpointTimestamps.UpdatedAt(checkpoint), checkpoint.ThreadId);
                if (admitted && query.Matches(checkpoint))
                {
                    var length = await reads[i].Length.ConfigureAwait(false);
                    threads.Add(ThreadSummary.Of(length - 1, checkpoint));
                    lastReturned = chunk[i];
                }
            }

            if (threads.Count == query.Limit)
            {
                break;
            }
        }

        return new Examination(threads, examined, lastExamined, lastReturned);
    }

    public async ValueTask<CheckpointLogPage> GetLogAsync(
        string threadId, long? afterSequence = null, int limit = ThreadQuery.DefaultLimit, CancellationToken cancellationToken = default)
    {
        ThreadQuery.CheckLimit(limit);
        var start = Math.Max(0, (afterSequence ?? -1) + 1);
        var decoded = await RunAsync(
                "load",
                async () => await CheckpointEncoding.DecodePageAsync(
                    await RangeAsync(threadId, start, start + limit - 1).ConfigureAwait(false),
                    async (from, to) => await RangeAsync(threadId, from, to).ConfigureAwait(false),
                    ReaderFor(threadId),
                    cancellationToken).ConfigureAwait(false),
                cancellationToken)
            .ConfigureAwait(false);
        return CheckpointLogPage.For([.. decoded.Select(row => new CheckpointLogEntry(row.Sequence, row.Checkpoint))], limit);
    }

    /// <summary>
    /// Deletes the logs of threads that are done with (<c>design/34-Storage-Hardening.md</c>, "Retention"),
    /// oldest first. Candidates come from the named states' index sets; each is then deleted by
    /// <see cref="PurgeScript"/>, which decides from the thread's own latest checkpoint in the same atomic
    /// step, so a thread that ran again meanwhile, or that a lease holds, is kept. Its blob hash goes in the same
    /// script; blobs in another blob store go after it (<c>design/41-Checkpoint-Efficiency.md</c>, "Retention"),
    /// and a failure there is the purge's.
    /// </summary>
    public async ValueTask<int> PurgeAsync(
        DateTimeOffset olderThan,
        IReadOnlySet<CheckpointThreadState>? states = null,
        int limit = ThreadQuery.MaxLimit,
        CancellationToken cancellationToken = default)
    {
        ThreadQuery.CheckLimit(limit);
        states ??= CheckpointRetention.Finished;
        if (states.Count == 0)
        {
            return 0;
        }

        var below = Score(CheckpointTimestamps.Text(olderThan));
        RedisValue[] arguments = [RedisValue.Null, below, .. states.Select(state => (RedisValue)state.Name())];

        // The script decides each thread, so some candidates are kept (held, or changed since they were
        // indexed). They take no place in the limit: the next round looks past them.
        var kept = new HashSet<string>(StringComparer.Ordinal);
        var purged = 0;
        while (purged < limit)
        {
            var wanted = limit - purged + kept.Count;
            var candidates = new List<(double Score, string ThreadId)>();
            foreach (var state in states.Distinct().Order())
            {
                var oldest = await RunAsync(
                    "purge",
                    () => _database.SortedSetRangeByScoreWithScoresAsync(
                        StateKey(state), double.NegativeInfinity, below, Exclude.Stop, Order.Ascending, 0, wanted),
                    cancellationToken).ConfigureAwait(false);
                candidates.AddRange(oldest.Select(entry => (entry.Score, (string)entry.Element!)));
            }

            var round = candidates
                .Where(c => !kept.Contains(c.ThreadId))
                .OrderBy(c => c.Score)
                .ThenBy(c => c.ThreadId, StringComparer.Ordinal)
                .Take(limit - purged)
                .ToList();
            if (round.Count == 0)
            {
                break;
            }

            foreach (var (_, threadId) in round)
            {
                // Purging twice is purging once: a repeat finds the list gone and deletes nothing.
                RedisValue[] forThread = [threadId, .. arguments[1..]];
                if ((int)await RunAsync(
                        "purge",
                        () => _database.ScriptEvaluateAsync(
                            PurgeScript,
                            [.. IndexKeys(threadId), LeaseKey(threadId), LeaseTokenKey(threadId), BlobKey(threadId)],
                            forThread),
                        cancellationToken).ConfigureAwait(false) == 1)
                {
                    purged++;
                    _writer?.Forget(threadId);
                    if (_blobs is { } blobs)
                    {
                        await blobs.DeleteThreadAsync(threadId, cancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    kept.Add(threadId);
                }
            }
        }

        return purged;
    }

    /// <summary>Whether running checkpoints are stored as deltas.</summary>
    internal bool WritesDeltas => _writer?.WritesDeltas == true;

    /// <summary>Whether large values are offloaded, and where to when not to this Redis.</summary>
    internal (bool On, ICheckpointBlobStore? Blobs) Offloads => (_writer?.Offloads == true, _blobs);

    public void Dispose()
    {
        _indexing.Dispose();
        _ownedConnection?.Dispose();
    }
}
