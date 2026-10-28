using System.Runtime.CompilerServices;
using Datalium.Strata.Memory;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.Storage.Memory;

/// <summary>
/// The "embedded" tier from design/03-Architecture.md's memory blueprint, via EF Core's SQLite
/// provider (per CLAUDE.md's project-structure comment naming "EF Core / Dapper / Redis
/// providers" for this project). A single <see cref="HistoryDbContext"/> instance isn't
/// thread-safe for concurrent operations, so every access is serialized behind a
/// <see cref="SemaphoreSlim"/> — the async equivalent of the Rust adapter's <c>Mutex</c>.
/// </summary>
public sealed class EfCoreHistoryStore : IHistoryStore, IDisposable
{
    /// <summary>
    /// The table <see cref="HistoryDbContext"/>'s model maps to -- what <c>EnsureCreated()</c> used to
    /// emit, so databases created by earlier versions are still readable. (Not the layout
    /// <c>strata-memory</c>'s <c>SqliteHistoryStore</c> uses: that one is <c>history_records</c> with a
    /// lowercase role, so this store's files are not cross-runtime. The Postgres and Redis history
    /// stores are.)
    /// </summary>
    public const string HistoryTableDdl = """
        CREATE TABLE IF NOT EXISTS "HistoryRecords" (
            "SessionId" TEXT NOT NULL,
            "Sequence" INTEGER NOT NULL,
            "Role" TEXT NOT NULL,
            "Content" TEXT NOT NULL,
            "Timestamp" TEXT NOT NULL,
            CONSTRAINT "PK_HistoryRecords" PRIMARY KEY ("SessionId", "Sequence")
        )
        """;

    private readonly HistoryDbContext _db;
    private readonly SqliteConnection? _ownedConnection;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public EfCoreHistoryStore(HistoryDbContext db, SqliteConnection? ownedConnection = null)
    {
        _db = db;
        _ownedConnection = ownedConnection;

        // Not EnsureCreated(): that skips table creation whenever the database already has *any*
        // table, so a history store opened on a file an EfCoreCheckpointStore had already initialised
        // (the two share one SQLite file in demo 12, as the Rust stores do) would find no table.
        // CREATE TABLE IF NOT EXISTS is a no-op against a table that's already there.
        _db.Database.ExecuteSqlRaw(HistoryTableDdl);
    }

    /// <summary>
    /// A self-contained in-memory SQLite database for tests: owns and keeps open the
    /// <see cref="SqliteConnection"/> a SQLite in-memory database needs to stay alive for
    /// (closing the last connection to a <c>:memory:</c> database deletes it), and disposes it
    /// alongside the <see cref="HistoryDbContext"/> in <see cref="Dispose"/>.
    /// </summary>
    public static EfCoreHistoryStore CreateInMemory()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var options = new DbContextOptionsBuilder<HistoryDbContext>().UseSqlite(connection).Options;
        return new EfCoreHistoryStore(new HistoryDbContext(options), connection);
    }

    public async Task AppendAsync(string sessionId, ChatMessage message, CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct);
        try
        {
            var maxSequence = await _db.HistoryRecords
                .Where(r => r.SessionId == sessionId)
                .Select(r => (long?)r.Sequence)
                .MaxAsync(ct);

            _db.HistoryRecords.Add(new HistoryRecordEntity
            {
                SessionId = sessionId,
                Sequence = (maxSequence ?? -1) + 1,
                Role = message.Role.ToString(),
                Content = message.Content.ToString(),
                Timestamp = message.Timestamp.ToString("O"),
            });
            await _db.SaveChangesAsync(ct);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async IAsyncEnumerable<ChatMessage> GetHistoryAsync(
        string sessionId,
        int limit = 50,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        List<HistoryRecordEntity> records;
        await _gate.WaitAsync(ct);
        try
        {
            records = await _db.HistoryRecords
                .Where(r => r.SessionId == sessionId)
                .OrderByDescending(r => r.Sequence)
                .Take(limit)
                .ToListAsync(ct);
        }
        finally
        {
            _gate.Release();
        }

        // Queried DESC (to LIMIT to the most recent rows); callers want chronological order back.
        records.Reverse();
        foreach (var record in records)
        {
            yield return new ChatMessage(
                Enum.Parse<MessageRole>(record.Role),
                record.Content.AsMemory(),
                DateTimeOffset.Parse(record.Timestamp));
        }
    }

    public void Dispose()
    {
        _db.Dispose();
        _ownedConnection?.Dispose();
        _gate.Dispose();
    }
}
