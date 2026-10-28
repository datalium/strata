using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.Storage.Memory;

public sealed class HistoryDbContext(DbContextOptions<HistoryDbContext> options) : DbContext(options)
{
    public DbSet<HistoryRecordEntity> HistoryRecords => Set<HistoryRecordEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HistoryRecordEntity>().HasKey(e => new { e.SessionId, e.Sequence });
    }
}
