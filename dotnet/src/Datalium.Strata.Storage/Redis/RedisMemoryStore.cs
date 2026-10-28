using System.Diagnostics;
using System.Globalization;
using System.Text;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage.Postgres;
using StackExchange.Redis;

namespace Datalium.Strata.Storage.Redis;

/// <summary>
/// Options of <see cref="RedisMemoryStore"/> (<c>design/45-Long-Term-Memory.md</c>, "Redis"). Mirrors
/// <c>strata-memory</c>'s <c>RedisMemoryOptions</c>.
/// </summary>
public sealed record RedisMemoryOptions
{
    /// <summary>
    /// The length of every vector: 1 to <see cref="MemoryRecords.MaxDimensions"/>. The first store on a prefix
    /// records them in <c>{prefix}:memory:schema</c> and the index's <c>DIM</c>, and a store opened on a prefix made
    /// with others is refused.
    /// </summary>
    public required int Dimensions { get; init; }

    /// <summary>
    /// Key namespace; keys are <c>{prefix}:memory:…</c>, beside the checkpoint store's <c>{prefix}:checkpoints:…</c>.
    /// An application with two embedding sizes uses two stores on two prefixes.
    /// </summary>
    public string Prefix { get; init; } = RedisConnection.DefaultPrefix;

    /// <summary>
    /// <see cref="MemoryIndex.None"/> (the default): the vector field is a <c>FLAT</c> index, so every search is exact.
    /// <see cref="MemoryIndex.Hnsw"/>: an approximate <c>HNSW</c> field, which may miss a near neighbour. Read when the
    /// index is created: an index that exists already is kept as it is.
    /// </summary>
    public MemoryIndex Index { get; init; } = MemoryIndex.None;

    /// <summary>How a call that failed for a transient reason is repeated. <see cref="StoreRetryPolicy.None"/> turns it off.</summary>
    public StoreRetryPolicy Retry { get; init; } = new();

    /// <summary>
    /// The clock for the records' timestamps and for the waits between attempts.
    /// <see langword="null"/>: <see cref="System.TimeProvider.System"/>.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }
}

/// <summary>
/// Redis <see cref="IMemoryStore"/> on the Query Engine (Redis 8, or Redis Stack) (<c>design/45-Long-Term-Memory.md</c>,
/// "Redis"). A record is a hash at <c>{prefix}:memory:records:{namespace}:{tenant}:{id}</c> (each part escaped, <c>%</c>
/// to <c>%25</c> then <c>:</c> to <c>%3A</c>), indexed by one <c>FT</c> index, <c>{prefix}:memory:idx</c>, on three tags
/// (<c>_scope</c>, <c>_id</c>, <c>_meta</c>: hex of the values' UTF-8, so a query needs no escaping) and the vector.
/// <c>{prefix}:memory:ids:{namespace}:{tenant}</c> lists a scope's ids for a listing in byte order, and
/// <c>{prefix}:memory:updated</c> every record by its <c>updated_at</c> for a purge. The layout is the one
/// <c>strata-memory</c>'s <c>RedisMemoryStore</c> makes, pinned by <c>schema/fixtures/memory/redis-record.json</c>,
/// so either runtime reads what the other wrote.
///
/// A search asks the index for twice the limit's nearest candidates (more when a tie spans the cut), with every filter
/// in the query, so filters apply before ranking, then scores each candidate from its stored vector in double
/// precision, as <see cref="InMemoryMemoryStore"/> does, and orders them by similarity and then id in byte order. An
/// upsert is one script per record (atomic for the record; <see cref="UpsertManyAsync"/> is not atomic across records),
/// and returns only once the index returns every record it wrote, with its own vector: the Query Engine indexes after
/// the write returns, so a search right after an upsert finds the record, as it would in the other stores. A record the
/// index hasn't returned 5 s after its write fails the upsert with
/// <c>memory upsert failed: memory on Redis didn't index record '&lt;id&gt;' within 5 s of writing it</c>; the record is
/// written all the same, and a repeat writes it again.
///
/// No constructor touches the server (<c>design/34-Storage-Hardening.md</c>): the first call, or
/// <see cref="MigrateAsync"/> or <see cref="ConnectAsync"/>, refuses a prefix whose <c>{prefix}:memory:schema</c> is
/// newer than this build or of other dimensions, records the version and dimensions, and then creates the index
/// (refusing with <c>memory on Redis needs the Query Engine (Redis 8 or Redis Stack): FT.CREATE is unknown</c> on a
/// server without it, which is left with the schema hash). Every call is repeated whole after a transient failure as
/// <see cref="RedisMemoryOptions.Retry"/> says (<c>strata.store.operation</c> <c>memory</c>), but for a purge, whose
/// reads and deletes are each repeated on their own. A failure is a <see cref="MemoryStoreException"/>.
///
/// Telemetry on the <see cref="StorageTelemetry.Name"/> meter: <c>strata.memory.operations</c>,
/// <c>strata.memory.operation.duration</c> and <c>strata.memory.search.results</c>, with <c>strata.store</c>
/// <c>redis</c>.
/// </summary>
public sealed class RedisMemoryStore : IMemoryStore, IMemoryRetention, IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Writes one record. <c>KEYS</c>: the record's hash, its scope's ids set, the updated set. <c>ARGV</c>: the id, the
    /// time (<c>created_at</c> when the record is new), the <c>updated_at</c> score, then the hash's other fields and
    /// values. Keeps an existing <c>created_at</c>, replaces every other field (a field the record no longer has is
    /// gone), and indexes the record in both sets. Identical to <c>strata-memory</c>'s <c>UPSERT_SCRIPT</c>.
    /// </summary>
    public const string UpsertScript = """
        local created = redis.call('HGET', KEYS[1], 'created_at') or ARGV[2]
        redis.call('DEL', KEYS[1])
        redis.call('HSET', KEYS[1], 'created_at', created, unpack(ARGV, 4))
        redis.call('ZADD', KEYS[2], 0, ARGV[1])
        redis.call('ZADD', KEYS[3], ARGV[3], KEYS[1])
        return 1
        """;

    /// <summary>
    /// Deletes one record. <c>KEYS</c>: the record's hash, its scope's ids set, the updated set. <c>ARGV</c>: the id,
    /// and a purge's cutoff (RFC 3339) or <c>''</c>: with one, a record whose <c>updated_at</c> isn't before it any more
    /// (written again since the purge read it) is kept, and 0 returned. Returns 1 when it deleted the hash, else 0.
    /// Identical to <c>strata-memory</c>'s <c>DELETE_SCRIPT</c>.
    /// </summary>
    public const string DeleteScript = """
        if ARGV[2] ~= '' then
          local updated = redis.call('HGET', KEYS[1], 'updated_at')
          if not updated or updated >= ARGV[2] then return 0 end
        end
        local existed = redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        redis.call('ZREM', KEYS[3], KEYS[1])
        return existed
        """;

    private const string StoreName = "redis";

    /// <summary>What the retry metric calls this store's calls.</summary>
    private const string RetryOperation = "memory";

    /// <summary>The most candidates a search asks the index for.</summary>
    internal const int MaxCandidates = 10_000;

    /// <summary>Two distances this close are a tie a search widens its candidates for.</summary>
    private const double TieTolerance = 1e-5;

    private readonly IDatabase _database;
    private readonly ConnectionMultiplexer? _ownedConnection;
    private readonly RedisMemoryOptions _options;
    private readonly string _prefix;
    private readonly TimeProvider _clock;
    private readonly StoreRetrier _retry;
    private readonly SemaphoreSlim _preparing = new(1, 1);
    private volatile bool _prepared;

    /// <param name="connection">An existing multiplexer, e.g. the application's shared one; the store doesn't own it.</param>
    /// <param name="options">The dimensions, the prefix, the index, retries and the clock.</param>
    public RedisMemoryStore(IConnectionMultiplexer connection, RedisMemoryOptions options)
        : this(Checked(connection).GetDatabase(), options)
    {
    }

    /// <param name="database">An existing database handle; the store doesn't own its connection.</param>
    /// <param name="options">The dimensions, the prefix, the index, retries and the clock.</param>
    public RedisMemoryStore(IDatabase database, RedisMemoryOptions options)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(options);
        Dimensions = MemoryRecords.CheckDimensions(options.Dimensions);
        ArgumentNullException.ThrowIfNull(options.Index);
        if (string.IsNullOrEmpty(options.Prefix))
        {
            throw new ArgumentException("the memory prefix is empty", nameof(options));
        }
        _database = database;
        _options = options;
        _prefix = options.Prefix;
        _clock = options.TimeProvider ?? TimeProvider.System;
        _retry = new StoreRetrier(options.Retry, options.TimeProvider, StoreName, RedisFaults.IsTransient);
    }

    private RedisMemoryStore(ConnectionMultiplexer connection, RedisMemoryOptions options)
        : this(connection.GetDatabase(), options)
    {
        _ownedConnection = connection;
    }

    /// <summary>
    /// Connects to <paramref name="url"/> (see <see cref="RedisConnection.Connect"/>), owns the connection, and
    /// prepares the prefix (<see cref="MigrateAsync"/>): the way to find out at startup that the server can't be
    /// reached, lacks the Query Engine, or holds memory of another build or size.
    /// </summary>
    public static async Task<RedisMemoryStore> ConnectAsync(
        string url, RedisMemoryOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var store = new RedisMemoryStore(await RedisConnection.ConnectAsync(url).ConfigureAwait(false), options);
        try
        {
            await store.MigrateAsync(cancellationToken).ConfigureAwait(false);
            return store;
        }
        catch
        {
            await store.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc/>
    public int Dimensions { get; }

    /// <summary>
    /// Prepares the prefix if the store hasn't yet: checks <c>{prefix}:memory:schema</c>, records the version and
    /// dimensions, checks them again, and creates the index. Every call does this first, so calling it is only a way to fail early. Safe
    /// to call at any time, from any number of processes at once.
    /// </summary>
    /// <exception cref="MemoryStoreException">
    /// The server lacks the Query Engine, the prefix is newer than this build, or it was made with other dimensions.
    /// </exception>
    public async Task MigrateAsync(CancellationToken cancellationToken = default) =>
        await PrepareAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public async ValueTask UpsertAsync(MemoryRecord record, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(record, Dimensions);
        await WriteAsync([record], cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// <see cref="UpsertAsync"/> for each of <paramref name="records"/>: every record is checked first, then each is
    /// written by a script of its own, all at one time, and the call returns once the index returns each (the last
    /// record of a key given twice). Not atomic across records (a script over many hashes would block the server
    /// through a large sync): a failure may leave some written.
    /// </summary>
    public async ValueTask UpsertManyAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(records, Dimensions);
        if (records.Count > 0)
        {
            await WriteAsync(records, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Writes <paramref name="records"/>, then waits until the index returns each of them (<see cref="ConfirmIndexedAsync"/>):
    /// the Query Engine indexes a hash after the write that made it returns, so without the wait a search right after an
    /// upsert could miss it. A failure of either is repeated whole, which is safe: an upsert is idempotent.
    /// </summary>
    private ValueTask<bool> WriteAsync(IReadOnlyList<MemoryRecord> records, CancellationToken cancellationToken)
    {
        // One time for every attempt: a repeat writes what the first would have.
        var now = MemoryRecords.TimestampText(_clock.GetUtcNow());
        var writes = records.Select(record => (Keys: RecordKeys(record.Key), Values: UpsertArguments(record, now))).ToList();
        var confirmations = LastOfEachKey(records).Select(Confirmation).ToList();
        return RunAsync(
            "upsert",
            async () =>
            {
                await Task.WhenAll(writes.Select(write => _database.ScriptEvaluateAsync(UpsertScript, write.Keys, write.Values)))
                    .ConfigureAwait(false);
                await ConfirmIndexedAsync(confirmations, cancellationToken).ConfigureAwait(false);
                return true;
            },
            cancellationToken);
    }

    /// <summary>How long after its write an upsert waits for the index to return a record before it fails.</summary>
    private static readonly TimeSpan IndexingBound = TimeSpan.FromSeconds(5);

    /// <summary>The first wait between rounds of confirmation queries; each wait doubles it, up to <see cref="MaxIndexingWait"/>.</summary>
    private static readonly TimeSpan FirstIndexingWait = TimeSpan.FromMilliseconds(1);

    /// <summary>The longest wait between rounds of confirmation queries.</summary>
    private static readonly TimeSpan MaxIndexingWait = TimeSpan.FromMilliseconds(50);

    /// <summary>The most a confirmation query's distance may be for the hit to be the record's own vector, not an older one.</summary>
    private const double IndexedDistance = 1e-4;

    /// <summary>
    /// The wait before round <paramref name="round"/> (from 1) of confirmation queries is repeated: 1 ms, then doubling,
    /// up to 50 ms.
    /// </summary>
    internal static TimeSpan IndexingWait(int round) =>
        TimeSpan.FromTicks(Math.Min(FirstIndexingWait.Ticks << Math.Clamp(round - 1, 0, 30), MaxIndexingWait.Ticks));

    /// <summary>The failure of an upsert whose record the index didn't return in time.</summary>
    internal static string NotIndexed(string id) => $"memory on Redis didn't index record '{id}' within 5 s of writing it";

    /// <summary>The records of an upsert whose writes stand: the last for each key, in the order they were written.</summary>
    private static List<MemoryRecord> LastOfEachKey(IReadOnlyList<MemoryRecord> records)
    {
        var seen = new HashSet<MemoryKey>();
        var last = new List<MemoryRecord>();
        for (var i = records.Count - 1; i >= 0; i--)
        {
            if (seen.Add(records[i].Key))
            {
                last.Add(records[i]);
            }
        }

        last.Reverse();
        return last;
    }

    /// <summary>A record's confirmation query: its id, its key, and the query's arguments.</summary>
    private sealed record IndexConfirmation(string Id, string Key, List<object> Arguments);

    private IndexConfirmation Confirmation(MemoryRecord record) =>
        new(record.Id, RecordKey(_prefix, record.Key), IndexedArguments(IndexKey, IndexedQuery(record), MemoryLayout.VectorBytes(record.Vector.Span)));

    /// <summary>
    /// Returns once the index returns every record of <paramref name="confirmations"/> with its own vector: each record's
    /// query at once, then the unconfirmed ones again, all together, after 1 ms, doubling up to 50 ms between rounds, for
    /// up to <see cref="IndexingBound"/> of real time (not the store's clock, which tests fake). Past it, a
    /// <see cref="TimeoutException"/> naming the first record still missing, which the call reports as
    /// <c>memory upsert failed: …</c>; the records are written all the same. The caller's token stops the wait.
    /// </summary>
    private async Task ConfirmIndexedAsync(List<IndexConfirmation> confirmations, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var pending = confirmations;
        var round = 0;
        while (true)
        {
            var confirmed = await Task.WhenAll(pending.Select(async confirmation =>
                    Indexed(await _database.ExecuteAsync("FT.SEARCH", confirmation.Arguments).ConfigureAwait(false), confirmation.Key)))
                .ConfigureAwait(false);
            pending = [.. pending.Where((_, i) => !confirmed[i])];
            if (pending.Count == 0)
            {
                return;
            }

            var left = IndexingBound - Stopwatch.GetElapsedTime(started);
            if (left <= TimeSpan.Zero)
            {
                throw new TimeoutException(NotIndexed(pending[0].Id));
            }

            var wait = IndexingWait(++round);
            await Task.Delay(wait < left ? wait : left, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Whether a confirmation query's <paramref name="reply"/> has the record at <paramref name="key"/> with its own vector.</summary>
    internal static bool Indexed(RedisResult reply, string key) =>
        ParseSearchReply(reply).Any(hit => hit.Key == key && hit.Distance <= IndexedDistance);

    /// <summary>
    /// A record's confirmation query: the tags a search builds for its scope, its id and each of its metadata pairs, then
    /// the KNN of the one candidate nearest to <c>$vec</c>, the record's own vector. So it finds the record only once its
    /// current scope, id, metadata and vector are all indexed.
    /// </summary>
    internal static string IndexedQuery(MemoryRecord record)
    {
        var query = new StringBuilder("(@_scope:{").Append(ScopeTag(record.Namespace, record.Tenant, record.Model)).Append('}');
        query.Append(" @_id:{").Append(Tag(record.Id)).Append('}');
        foreach (var (key, value) in record.Metadata.OrderBy(pair => pair.Key, MemoryRecords.ByteOrder))
        {
            query.Append(" @_meta:{").Append(Tag(key)).Append(Tag(value)).Append('}');
        }

        return query.Append(")=>[KNN 1 @embedding $vec AS __distance]").ToString();
    }

    /// <summary>A confirmation query's <c>FT.SEARCH</c> arguments after the command name: its distance alone, one document.</summary>
    internal static List<object> IndexedArguments(string index, string query, byte[] vector) =>
        [index, query, "PARAMS", "2", "vec", vector, "RETURN", "1", "__distance", "LIMIT", "0", "1", "TIMEOUT", "0", "DIALECT", "2"];

    /// <summary>The upsert script's arguments for <paramref name="record"/> written at <paramref name="now"/>.</summary>
    internal static RedisValue[] UpsertArguments(MemoryRecord record, string now)
    {
        var values = new List<RedisValue> { record.Id, now, Score(now) };
        foreach (var (field, value) in HashFields(record, now))
        {
            values.Add(field);
            values.Add(value);
        }

        return [.. values];
    }

    /// <summary>
    /// The record's hash fields but <c>created_at</c>, as the layout says: <c>content</c> and <c>text</c> left out when
    /// null, <c>_meta</c> when there is no metadata.
    /// </summary>
    internal static List<(string Field, RedisValue Value)> HashFields(MemoryRecord record, string updatedAt)
    {
        var fields = new List<(string, RedisValue)>
        {
            ("namespace", record.Namespace),
            ("tenant", record.Tenant),
            ("id", record.Id),
            ("model", record.Model),
            ("embedding", MemoryLayout.VectorBytes(record.Vector.Span)),
        };
        if (MemoryLayout.ContentJson(record.Content) is { } content)
        {
            fields.Add(("content", content));
        }

        if (record.Text is not null)
        {
            fields.Add(("text", record.Text));
        }

        fields.Add(("metadata", MemoryLayout.MetadataJson(record.Metadata)));
        fields.Add(("updated_at", updatedAt));
        fields.Add(("_scope", ScopeTag(record.Namespace, record.Tenant, record.Model)));
        fields.Add(("_id", Tag(record.Id)));
        if (record.Metadata.Count > 0)
        {
            fields.Add(("_meta", MetaTags(record.Metadata)));
        }

        return fields;
    }

    /// <inheritdoc/>
    public ValueTask<MemoryRecord?> GetAsync(MemoryKey key, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(key);
        var recordKey = RecordKey(key);
        return RunAsync(
            "get",
            async () => ReadRecord(await _database.HashGetAllAsync(recordKey).ConfigureAwait(false)),
            cancellationToken);
    }

    /// <summary>
    /// Deletes the record at <paramref name="key"/>; whether there was one. Repeated after a transient failure: when the
    /// failure lost the answer of a delete that happened, the repeat finds nothing and answers <see langword="false"/>.
    /// </summary>
    public ValueTask<bool> DeleteAsync(MemoryKey key, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(key);
        var keys = RecordKeys(key);
        return RunAsync(
            "delete",
            async () => (long)await _database.ScriptEvaluateAsync(DeleteScript, keys, [key.Id, ""]).ConfigureAwait(false) == 1,
            cancellationToken);
    }

    /// <summary>
    /// The records most similar to <paramref name="search"/>'s vector. The index's KNN query takes every filter as a
    /// pre-filter (scope, excluded ids, metadata) and returns twice the limit's candidates, or more while a tie spans
    /// the cut, as keys and distances only; each candidate is then read from its hash, checked against the search again
    /// (it may have been written since the index answered), scored from its stored vector in double precision, ordered
    /// by similarity and then id in byte order, cut at <see cref="MemorySearch.MinSimilarity"/> and the limit.
    /// </summary>
    public async ValueTask<IReadOnlyList<MemoryMatch>> SearchAsync(MemorySearch search, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(search, Dimensions);
        var query = SearchQuery(search);
        var vector = MemoryLayout.VectorBytes(search.Vector.Span);
        var matches = await RunAsync(
                "search",
                async () =>
                {
                    var candidates = Math.Min(2 * search.Limit, MaxCandidates);
                    while (true)
                    {
                        var reply = await _database.ExecuteAsync("FT.SEARCH", SearchArguments(IndexKey, query, vector, candidates))
                            .ConfigureAwait(false);
                        var found = ParseSearchReply(reply);
                        if (found.Count < candidates || candidates >= MaxCandidates || !TieAtTheCut(found, search.Limit))
                        {
                            // Each candidate is read from its hash, all together in one pipeline: a reply that also
                            // returns the documents' fields can leave documents out on Redis 8.
                            var hashes = await Task.WhenAll(found.Select(hit => _database.HashGetAllAsync(hit.Key)))
                                .ConfigureAwait(false);
                            return Rank([.. hashes.Select(ReadRecord)], search);
                        }

                        candidates = Math.Min(2 * candidates, MaxCandidates);
                    }
                },
                cancellationToken)
            .ConfigureAwait(false);

        StorageTelemetry.MemorySearched(StoreName, matches.Count);
        return matches;
    }

    /// <summary>Whether the last candidate is as near as the one at the limit: a tie may continue past them.</summary>
    private static bool TieAtTheCut(List<SearchHit> found, int limit)
    {
        var atLimit = found[Math.Min(limit, found.Count) - 1].Distance;
        return Math.Abs(found[^1].Distance - atLimit) <= TieTolerance;
    }

    private static List<MemoryMatch> Rank(MemoryRecord?[] records, MemorySearch search) =>
    [
        .. records
            .OfType<MemoryRecord>()
            // Read again when the reply lacked fields, so possibly written again since the index answered: checked again.
            .Where(record => record.Namespace == search.Namespace
                && record.Tenant == search.Tenant
                && record.Model == search.Model
                && record.Vector.Length == search.Vector.Length
                && !search.Exclude.Contains(record.Id)
                && MemoryRecords.Matches(record.Metadata, search.Metadata))
            .Select(record => new MemoryMatch(record, MemoryRecords.CosineSimilarity(record.Vector.Span, search.Vector.Span)))
            .Where(match => search.MinSimilarity is not { } min || match.Similarity >= min)
            .OrderByDescending(match => match.Similarity)
            .ThenBy(match => match.Record.Id, MemoryRecords.ByteOrder)
            .Take(search.Limit),
    ];

    /// <inheritdoc/>
    public ValueTask<MemoryPage> ListAsync(MemoryQuery query, CancellationToken cancellationToken = default)
    {
        var after = MemoryRecords.Check(query);
        var idsKey = IdsKey(query.Namespace, query.Tenant);
        var chunk = Math.Clamp(query.Limit + 1, 32, MemoryRecords.MaxLimit);
        return RunAsync(
            "list",
            async () =>
            {
                var records = new List<MemoryRecord>();
                var from = after;
                while (records.Count <= query.Limit)
                {
                    var ids = await _database.SortedSetRangeByValueAsync(
                            idsKey,
                            min: from is null ? default : (RedisValue)from,
                            max: default,
                            exclude: from is null ? Exclude.None : Exclude.Start,
                            order: Order.Ascending,
                            skip: 0,
                            take: chunk)
                        .ConfigureAwait(false);
                    var hashes = await Task.WhenAll(ids.Select(id =>
                            _database.HashGetAllAsync(RecordKey(new MemoryKey(query.Namespace, query.Tenant, (string)id!)))))
                        .ConfigureAwait(false);
                    foreach (var hash in hashes)
                    {
                        if (ReadRecord(hash) is { } record && MemoryRecords.Matches(record.Metadata, query.Metadata))
                        {
                            records.Add(record);
                            if (records.Count > query.Limit)
                            {
                                break;
                            }
                        }
                    }

                    if (ids.Length < chunk)
                    {
                        break;
                    }

                    from = (string)ids[^1]!;
                }

                string? next = null;
                if (records.Count > query.Limit)
                {
                    records.RemoveRange(query.Limit, records.Count - query.Limit);
                    next = MemoryRecords.EncodeCursor(records[^1].Id);
                }

                return new MemoryPage(records, next);
            },
            cancellationToken);
    }

    /// <summary>
    /// Deletes the records <paramref name="purge"/> names, oldest first and then by namespace, tenant and id: reads
    /// <c>{prefix}:memory:updated</c> below the cutoff (at the second, exclusive) a page at a time, one second at a time
    /// (a second that fills a page is read whole), keeps the records whose namespace, tenant and <c>updated_at</c> match,
    /// and deletes each with a script that keeps a record written again since it was read; such a record doesn't count,
    /// and the next one takes its place. Stops when <see cref="MemoryPurge.Limit"/> records are deleted. Each read and
    /// each delete is repeated on its own after a transient failure, never the whole purge, so the count is the records
    /// this call deleted; a delete whose answer the failure lost is found done when repeated and not counted, so the
    /// call may then delete one more than it reports. As <c>strata-memory</c>'s purge does.
    /// </summary>
    public ValueTask<int> PurgeAsync(MemoryPurge purge, CancellationToken cancellationToken = default)
    {
        MemoryRecords.Check(purge);
        var cutoff = MemoryRecords.TimestampText(purge.OlderThan);
        var cutoffScore = Score(cutoff);
        return ObserveAsync(
            "purge",
            async () =>
            {
                var deleted = 0;
                var lower = double.NegativeInfinity;
                var bounds = Exclude.Stop;
                while (true)
                {
                    var (from, fromBounds) = (lower, bounds);
                    var page = await RetryAsync(
                            () => _database.SortedSetRangeByScoreWithScoresAsync(
                                UpdatedKey, from, cutoffScore, fromBounds, Order.Ascending, 0, PurgePage),
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (page.Length == 0)
                    {
                        return deleted;
                    }

                    var full = page.Length == PurgePage;
                    var groups = await SecondsAsync(page, full, cancellationToken).ConfigureAwait(false);
                    foreach (var (score, members) in groups)
                    {
                        foreach (var key in await DoomedAsync(members, purge, cutoff, cancellationToken).ConfigureAwait(false))
                        {
                            if (deleted == purge.Limit)
                            {
                                return deleted;
                            }

                            var result = await RetryAsync(
                                    () => _database.ScriptEvaluateAsync(DeleteScript, RecordKeys(key), [key.Id, cutoff]),
                                    cancellationToken)
                                .ConfigureAwait(false);
                            if ((long)result == 1)
                            {
                                deleted++;
                            }
                        }

                        lower = score;
                        bounds = Exclude.Both;
                    }

                    if (!full)
                    {
                        return deleted;
                    }
                }
            },
            cancellationToken);
    }

    /// <summary>How many members of the updated set a purge reads at a time.</summary>
    private const int PurgePage = 500;

    /// <summary>
    /// A purge page's members by second, in the set's order. A full page's last second may go on past it, so it is left
    /// for the next page, unless it is the page's only second, which is then read whole.
    /// </summary>
    private async Task<List<(double Score, List<string> Members)>> SecondsAsync(
        SortedSetEntry[] page, bool full, CancellationToken cancellationToken)
    {
        var groups = new List<(double Score, List<string> Members)>();
        foreach (var entry in page)
        {
            if (groups.Count > 0 && groups[^1].Score == entry.Score)
            {
                groups[^1].Members.Add((string)entry.Element!);
            }
            else
            {
                groups.Add((entry.Score, [(string)entry.Element!]));
            }
        }

        if (!full)
        {
            return groups;
        }

        if (groups.Count > 1)
        {
            groups.RemoveAt(groups.Count - 1);
            return groups;
        }

        var at = groups[0].Score;
        var whole = await RetryAsync(() => _database.SortedSetRangeByScoreAsync(UpdatedKey, at, at), cancellationToken)
            .ConfigureAwait(false);
        return [(at, [.. whole.Select(member => (string)member!)])];
    }

    /// <summary>
    /// The records of one second's members that <paramref name="purge"/> deletes, in the order it deletes them: those
    /// whose hash is still there with a matching namespace and tenant and an <c>updated_at</c> before the cutoff.
    /// </summary>
    private async Task<List<MemoryKey>> DoomedAsync(
        List<string> members, MemoryPurge purge, string cutoff, CancellationToken cancellationToken)
    {
        var fields = await RetryAsync(
                () => Task.WhenAll(members.Select(member =>
                    _database.HashGetAsync(member, ["namespace", "tenant", "id", "updated_at"]))),
                cancellationToken)
            .ConfigureAwait(false);
        return
        [
            .. fields
                .Where(found => found is [{ IsNull: false }, { IsNull: false }, { IsNull: false }, { IsNull: false }])
                .Select(found => (Key: new MemoryKey((string)found[0]!, (string)found[1]!, (string)found[2]!), Updated: (string)found[3]!))
                .Where(found => (purge.Namespace is null || purge.Namespace == found.Key.Namespace)
                    && (purge.Tenant is null || purge.Tenant == found.Key.Tenant)
                    && string.CompareOrdinal(found.Updated, cutoff) < 0)
                .OrderBy(found => found.Updated, MemoryRecords.ByteOrder)
                .ThenBy(found => found.Key.Namespace, MemoryRecords.ByteOrder)
                .ThenBy(found => found.Key.Tenant, MemoryRecords.ByteOrder)
                .ThenBy(found => found.Key.Id, MemoryRecords.ByteOrder)
                .Select(found => found.Key),
        ];
    }

    /// <summary>
    /// Makes one call: prepares the prefix, then runs <paramref name="call"/> under the retry policy, whole; counts it,
    /// and turns a failure into a <see cref="MemoryStoreException"/>.
    /// </summary>
    private ValueTask<T> RunAsync<T>(string operation, Func<Task<T>> call, CancellationToken cancellationToken) =>
        ObserveAsync(operation, () => RetryAsync(call, cancellationToken), cancellationToken);

    /// <summary>Runs one step of a call under the retry policy.</summary>
    private Task<T> RetryAsync<T>(Func<Task<T>> step, CancellationToken cancellationToken) =>
        _retry.RunAsync(RetryOperation, (_, _) => new ValueTask<T>(step()), cancellationToken).AsTask();

    /// <summary>
    /// Makes one call that repeats its own steps: prepares the prefix, runs <paramref name="call"/>, counts it, and turns
    /// a failure into a <see cref="MemoryStoreException"/>.
    /// </summary>
    private async ValueTask<T> ObserveAsync<T>(string operation, Func<Task<T>> call, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await PrepareAsync(cancellationToken).ConfigureAwait(false);
            return await call().ConfigureAwait(false);
        }
        catch (Exception e) when (e is not (OperationCanceledException or MemoryStoreException))
        {
            throw new MemoryStoreException($"memory {operation} failed: {e.Message}", e);
        }
        finally
        {
            StorageTelemetry.MemoryOperation(StoreName, operation, Stopwatch.GetElapsedTime(started));
        }
    }

    /// <summary>Prepares the prefix the first time the store is used. A failure is the call's, and the next call tries again.</summary>
    private async ValueTask PrepareAsync(CancellationToken cancellationToken)
    {
        if (_prepared)
        {
            return;
        }

        await _preparing.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_prepared)
            {
                return;
            }

            await _retry.RunAsync(RetryOperation, (_, _) => new ValueTask(PrepareOnceAsync()), cancellationToken).ConfigureAwait(false);
            _prepared = true;
        }
        catch (Exception e) when (e is not (OperationCanceledException or MemoryStoreException))
        {
            throw new MemoryStoreException($"memory couldn't be prepared: {e.Message}", e);
        }
        finally
        {
            _preparing.Release();
        }
    }

    /// <summary>
    /// Refuses a newer prefix or one of other dimensions; records the version and dimensions unless they are, and
    /// checks them again, since another store may have recorded its own first; and only then creates the index (an
    /// existing one is kept). That order means the index is only ever made at the dimensions the prefix records: two
    /// stores of different sizes opening at once can't leave one of them on an index of the other's size. On a server
    /// without the Query Engine it leaves <c>{prefix}:memory:schema</c> behind, which is harmless.
    /// </summary>
    private async Task PrepareOnceAsync()
    {
        CheckSchema(await _database.HashGetAsync(SchemaKey, ["version", "dimensions"]).ConfigureAwait(false));
        await _database.HashSetAsync(SchemaKey, "version", StoreSchemaVersions.MemoryVersion, When.NotExists).ConfigureAwait(false);
        await _database.HashSetAsync(SchemaKey, "dimensions", Dimensions, When.NotExists).ConfigureAwait(false);
        CheckSchema(await _database.HashGetAsync(SchemaKey, ["version", "dimensions"]).ConfigureAwait(false));
        try
        {
            await _database.ExecuteAsync("FT.CREATE", FtCreateArguments(_prefix, Dimensions, _options.Index)).ConfigureAwait(false);
        }
        catch (RedisServerException e) when (IndexExists(e))
        {
            // Made by an earlier store on this prefix.
        }
        catch (RedisServerException e) when (UnknownCommand(e))
        {
            throw new MemoryStoreException(NoQueryEngine, e);
        }
    }

    /// <summary>The refusal on a server without the Query Engine.</summary>
    internal const string NoQueryEngine = "memory on Redis needs the Query Engine (Redis 8 or Redis Stack): FT.CREATE is unknown";

    private void CheckSchema(RedisValue[] schema)
    {
        if (schema[0].HasValue && long.Parse((string)schema[0]!, CultureInfo.InvariantCulture) is var version
            && version > StoreSchemaVersions.MemoryVersion)
        {
            throw new MemoryStoreException(StoreSchemaVersions.Newer(StoreSchemaVersions.Memory, version, StoreSchemaVersions.MemoryVersion));
        }

        if (schema[1].HasValue && long.Parse((string)schema[1]!, CultureInfo.InvariantCulture) is var dimensions
            && dimensions != Dimensions)
        {
            throw new MemoryStoreException(
                $"memory was created with {dimensions.ToString(CultureInfo.InvariantCulture)} dimensions, and this store was opened with {Dimensions.ToString(CultureInfo.InvariantCulture)}");
        }
    }

    /// <summary>Whether <c>FT.CREATE</c> failed because the index exists already.</summary>
    internal static bool IndexExists(RedisServerException e) =>
        e.Message.Contains("Index already exists", StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether a command failed because the server doesn't know it: a Redis without the Query Engine.</summary>
    internal static bool UnknownCommand(RedisServerException e) =>
        e.Message.Contains("unknown command", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// <c>FT.CREATE</c>'s arguments after the command name, pinned by <c>redis-record.json</c>'s <c>ft_create</c> (and,
    /// for the vector field with an HNSW index, <c>ft_create_hnsw</c>).
    /// </summary>
    internal static List<object> FtCreateArguments(string prefix, int dimensions, MemoryIndex index)
    {
        var dim = dimensions.ToString(CultureInfo.InvariantCulture);
        List<object> arguments =
        [
            $"{prefix}:memory:idx", "ON", "HASH", "PREFIX", "1", $"{prefix}:memory:records:",
            "SCHEMA",
            "_scope", "TAG",
            "_id", "TAG",
            "_meta", "TAG",
            "embedding", "VECTOR",
        ];
        if (index is MemoryIndex.HnswIndex hnsw)
        {
            arguments.AddRange(
            [
                "HNSW", "10", "TYPE", "FLOAT32", "DIM", dim, "DISTANCE_METRIC", "COSINE",
                "M", hnsw.M.ToString(CultureInfo.InvariantCulture),
                "EF_CONSTRUCTION", hnsw.EfConstruction.ToString(CultureInfo.InvariantCulture),
            ]);
        }
        else
        {
            arguments.AddRange(["FLAT", "6", "TYPE", "FLOAT32", "DIM", dim, "DISTANCE_METRIC", "COSINE"]);
        }

        return arguments;
    }

    /// <summary>
    /// A search's query: the scope's tag, the excluded ids, one clause per metadata pair, then the KNN of
    /// <c>$k</c> candidates nearest to <c>$vec</c>.
    /// </summary>
    internal static string SearchQuery(MemorySearch search)
    {
        var query = new StringBuilder("(@_scope:{").Append(ScopeTag(search.Namespace, search.Tenant, search.Model)).Append('}');
        if (search.Exclude.Count > 0)
        {
            query.Append(" -@_id:{").AppendJoin('|', search.Exclude.Distinct(StringComparer.Ordinal).Select(Tag)).Append('}');
        }

        foreach (var (key, value) in search.Metadata.OrderBy(pair => pair.Key, MemoryRecords.ByteOrder))
        {
            query.Append(" @_meta:{").Append(Tag(key)).Append(Tag(value)).Append('}');
        }

        return query.Append(")=>[KNN $k @embedding $vec AS __distance]").ToString();
    }

    /// <summary>
    /// <c>FT.SEARCH</c>'s arguments after the command name, for <paramref name="candidates"/> candidates: the keys and
    /// their distances only. A reply that also returns the documents' fields can leave documents out on Redis 8, at
    /// about one search in twenty straight after a write under load; this one hasn't been seen to, so the records are
    /// read from their hashes after it.
    /// </summary>
    internal static List<object> SearchArguments(string index, string query, byte[] vector, int candidates)
    {
        var k = candidates.ToString(CultureInfo.InvariantCulture);
        // TIMEOUT 0: no time limit, so the Query Engine's default timeout can't return a partial top K as if it were whole.
        return
        [
            index, query, "PARAMS", "4", "k", k, "vec", vector, "SORTBY", "__distance", "ASC", "RETURN", "1", "__distance",
            "LIMIT", "0", k, "TIMEOUT", "0", "DIALECT", "2",
        ];
    }

    /// <summary>One document of an <c>FT.SEARCH</c> reply: its key, the fields it returned, and its distance.</summary>
    internal sealed record SearchHit(string Key, HashEntry[] Fields, double Distance);

    /// <summary>
    /// The documents of an <c>FT.SEARCH</c> reply, in order: RESP2's <c>[total, key, [field, value, …], …]</c>, or
    /// RESP3's map whose <c>results</c> hold each document's <c>id</c> and <c>extra_attributes</c>.
    /// </summary>
    internal static List<SearchHit> ParseSearchReply(RedisResult reply)
    {
        var hits = new List<SearchHit>();
        if (reply.Resp3Type == ResultType.Map)
        {
            var results = Entry(reply, "results");
            if (results is null || results.IsNull)
            {
                return hits;
            }

            foreach (var document in (RedisResult[])results!)
            {
                var key = (string)Entry(document, "id")!;
                var attributes = Entry(document, "extra_attributes");
                hits.Add(Hit(key!, attributes is null || attributes.IsNull ? [] : (RedisResult[])attributes!));
            }

            return hits;
        }

        // With RETURN, every key is followed by its fields: an array, or nil for a document deleted while the query
        // ran, which is kept without fields (and dropped when its hash is read), never taken for the next key.
        var items = (RedisResult[])reply!;
        for (var i = 1; i < items.Length; i += 2)
        {
            var key = (string)items[i]!;
            var fields = i + 1 < items.Length && !items[i + 1].IsNull && items[i + 1].Resp2Type == ResultType.Array
                ? (RedisResult[])items[i + 1]!
                : [];
            hits.Add(Hit(key!, fields));
        }

        return hits;
    }

    private static SearchHit Hit(string key, RedisResult[] pairs)
    {
        var fields = new List<HashEntry>();
        var distance = double.PositiveInfinity;
        for (var i = 0; i + 1 < pairs.Length; i += 2)
        {
            var name = (string)pairs[i]!;
            if (name == "__distance")
            {
                distance = double.Parse((string)pairs[i + 1]!, NumberStyles.Float, CultureInfo.InvariantCulture);
            }
            else
            {
                fields.Add(new HashEntry(name, (RedisValue)pairs[i + 1]));
            }
        }

        return new SearchHit(key, [.. fields], distance);
    }

    /// <summary>The value at <paramref name="name"/> in a RESP3 map (a flat array of keys and values).</summary>
    private static RedisResult? Entry(RedisResult map, string name)
    {
        var items = (RedisResult[])map!;
        for (var i = 0; i + 1 < items.Length; i += 2)
        {
            if ((string)items[i]! == name)
            {
                return items[i + 1];
            }
        }

        return null;
    }


    /// <summary>A record from its hash fields; <see langword="null"/> when the hash is gone.</summary>
    private static MemoryRecord? ReadRecord(HashEntry[] hash)
    {
        if (hash.Length == 0)
        {
            return null;
        }

        var fields = hash.ToDictionary(entry => (string)entry.Name!, entry => entry.Value, StringComparer.Ordinal);
        if (!fields.ContainsKey("id"))
        {
            return null;
        }

        string? Optional(string name) => fields.TryGetValue(name, out var value) && !value.IsNull ? (string?)value : null;
        string Required(string name) => Optional(name) ?? throw new FormatException($"a memory record's hash has no '{name}'");
        return new MemoryRecord(
            Required("namespace"),
            Required("tenant"),
            Required("id"),
            MemoryLayout.Vector((byte[])fields["embedding"]!),
            Required("model"))
        {
            Content = MemoryLayout.ParseContent(Optional("content")),
            Text = Optional("text"),
            Metadata = MemoryLayout.ParseMetadata(Optional("metadata") ?? "{}"),
            CreatedAt = MemoryRecords.ParseTimestamp(Required("created_at")),
            UpdatedAt = MemoryRecords.ParseTimestamp(Required("updated_at")),
        };
    }

    /// <summary>A key part escaped so that <c>:</c> only ever separates parts: <c>%</c> to <c>%25</c>, then <c>:</c> to <c>%3A</c>.</summary>
    internal static string Escape(string part) => part.Replace("%", "%25", StringComparison.Ordinal).Replace(":", "%3A", StringComparison.Ordinal);

    /// <summary>A tag: <c>x</c> and the lowercase hex of <paramref name="value"/>'s UTF-8, so it is never empty and needs no escaping.</summary>
    internal static string Tag(string value) => "x" + Convert.ToHexStringLower(Encoding.UTF8.GetBytes(value));

    /// <summary>The <c>_scope</c> tag: the namespace's, the tenant's and the model's, one after another.</summary>
    internal static string ScopeTag(string ns, string tenant, string model) => Tag(ns) + Tag(tenant) + Tag(model);

    /// <summary>The <c>_meta</c> tags: the key's and the value's tag per pair, keys in byte order, joined by <c>,</c>.</summary>
    internal static string MetaTags(IReadOnlyDictionary<string, string> metadata) =>
        string.Join(',', metadata.OrderBy(pair => pair.Key, MemoryRecords.ByteOrder).Select(pair => Tag(pair.Key) + Tag(pair.Value)));

    /// <summary>A timestamp's score in the updated set: its digits (<c>2026-10-21T14:05:47Z</c> is <c>20261021140547</c>).</summary>
    internal static double Score(string timestamp)
    {
        var digits = new StringBuilder(14);
        foreach (var c in timestamp)
        {
            if (char.IsAsciiDigit(c))
            {
                digits.Append(c);
            }
        }

        return double.Parse(digits.ToString(), CultureInfo.InvariantCulture);
    }

    /// <summary>A record's hash key.</summary>
    internal static string RecordKey(string prefix, MemoryKey key) =>
        $"{prefix}:memory:records:{Escape(key.Namespace)}:{Escape(key.Tenant)}:{Escape(key.Id)}";

    /// <summary>A scope's ids set.</summary>
    internal static string IdsKey(string prefix, string ns, string tenant) => $"{prefix}:memory:ids:{Escape(ns)}:{Escape(tenant)}";

    private RedisKey RecordKey(MemoryKey key) => RecordKey(_prefix, key);

    private RedisKey IdsKey(string ns, string tenant) => IdsKey(_prefix, ns, tenant);

    private RedisKey[] RecordKeys(MemoryKey key) => [RecordKey(key), IdsKey(key.Namespace, key.Tenant), UpdatedKey];

    private RedisKey UpdatedKey => $"{_prefix}:memory:updated";

    private RedisKey SchemaKey => $"{_prefix}:memory:schema";

    private string IndexKey => $"{_prefix}:memory:idx";

    private static IConnectionMultiplexer Checked(IConnectionMultiplexer connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        return connection;
    }

    public void Dispose()
    {
        _preparing.Dispose();
        _ownedConnection?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        _preparing.Dispose();
        if (_ownedConnection is not null)
        {
            await _ownedConnection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
