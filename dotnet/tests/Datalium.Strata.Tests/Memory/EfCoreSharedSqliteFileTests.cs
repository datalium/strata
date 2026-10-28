using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage.Memory;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.Tests.Memory;

/// <summary>
/// <see cref="EfCoreCheckpointStore"/> and <see cref="EfCoreHistoryStore"/> on one SQLite file, which is what
/// <c>strata-memory</c>'s two SQLite stores already supported and what demo 12 does. It used to fail when the
/// checkpoint store initialised the file first: the history store's <c>EnsureCreated()</c> skips table
/// creation whenever the database already has any table, so its first query hit "no such table".
/// </summary>
public sealed class EfCoreSharedSqliteFileTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"strata-shared-file-{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        // Microsoft.Data.Sqlite pools connections, which keeps the file open after a store is disposed.
        SqliteConnection.ClearAllPools();
        try
        {
            File.Delete(_path);
        }
        catch (IOException)
        {
            // Best-effort cleanup of a temp file.
        }
    }

    private static DbContextOptions<TContext> Options<TContext>(string path)
        where TContext : DbContext =>
        new DbContextOptionsBuilder<TContext>().UseSqlite($"Data Source={path}").Options;

    private EfCoreCheckpointStore OpenCheckpoints() => new(new CheckpointDbContext(Options<CheckpointDbContext>(_path)));

    private EfCoreHistoryStore OpenHistory() => new(new HistoryDbContext(Options<HistoryDbContext>(_path)));

    private static Checkpoint Sample() => new()
    {
        SchemaVersion = "1.0.0",
        ThreadId = "thread-1",
        Status = CheckpointStatus.Interrupted,
        SuspendedNode = "A",
        NextNode = "B",
        FrozenState = JsonNode.Parse("""{"x":1}"""),
        CreatedAt = "2026-09-20T00:00:00Z",
    };

    private static async Task<string?> OnlyMessageAsync(IHistoryStore history)
    {
        var contents = new List<string>();
        await foreach (var message in history.GetHistoryAsync("s1"))
        {
            contents.Add(message.Content.ToString());
        }

        return contents.SingleOrDefault();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BothStoresWorkOnOneFileInEitherOpenOrder(bool checkpointsFirst)
    {
        var (checkpoints, history) = checkpointsFirst
            ? (OpenCheckpoints(), OpenHistory())
            : SwapOrder();

        using (checkpoints)
        using (history)
        {
            checkpoints.Save(Sample());
            await history.AppendAsync("s1", new ChatMessage(MessageRole.User, "hello".AsMemory(), DateTimeOffset.UtcNow));

            Assert.Equal("thread-1", checkpoints.Load("thread-1")?.ThreadId);
            Assert.Equal("hello", await OnlyMessageAsync(history));
        }

        // ... and a brand-new pair of instances (a restarted process) sees both.
        using var checkpointsAgain = OpenCheckpoints();
        using var historyAgain = OpenHistory();
        Assert.NotNull(checkpointsAgain.Load("thread-1"));
        Assert.Equal("hello", await OnlyMessageAsync(historyAgain));

        (EfCoreCheckpointStore, EfCoreHistoryStore) SwapOrder()
        {
            var h = OpenHistory();
            return (OpenCheckpoints(), h);
        }
    }

    [Fact]
    public void TheHistoryTableDdlIsWhatEfCoreItselfWouldCreateForTheModel()
    {
        // The literal DDL replaced EnsureCreated(), so databases created by earlier versions must stay
        // readable: guard against the literal drifting from the EF model.
        using var db = new HistoryDbContext(Options<HistoryDbContext>(_path));
        static string Normalize(string sql) => Regex.Replace(sql, @"\s+", " ").Replace(" (", "(").Replace("( ", "(").Replace(" )", ")").Trim(' ', ';');

        Assert.Equal(
            Normalize(db.Database.GenerateCreateScript()),
            Normalize(EfCoreHistoryStore.HistoryTableDdl.Replace("IF NOT EXISTS ", string.Empty)));
    }
}
