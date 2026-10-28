using Microsoft.EntityFrameworkCore;

namespace Datalium.Strata.Storage.Memory;

public sealed class CheckpointDbContext(DbContextOptions<CheckpointDbContext> options) : DbContext(options)
{
    public DbSet<CheckpointRecordEntity> Checkpoints => Set<CheckpointRecordEntity>();

    /// <summary>
    /// Maps onto the shared checkpoint-log table by name. The table itself is created by
    /// <see cref="EfCoreCheckpointStore"/> from one literal DDL string rather than by EF's
    /// <c>EnsureCreated</c>, so the layout is exactly the one the Rust store and the CLI create.
    /// </summary>
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CheckpointRecordEntity>(entity =>
        {
            entity.ToTable("checkpoints");
            entity.HasKey(e => new { e.ThreadId, e.Sequence });
            entity.Property(e => e.ThreadId).HasColumnName("thread_id");
            entity.Property(e => e.Sequence).HasColumnName("sequence");
            entity.Property(e => e.SchemaVersion).HasColumnName("schema_version");
            entity.Property(e => e.Status).HasColumnName("status");
            entity.Property(e => e.SuspendedNode).HasColumnName("suspended_node");
            entity.Property(e => e.NextNode).HasColumnName("next_node");
            entity.Property(e => e.FrozenState).HasColumnName("frozen_state");
            entity.Property(e => e.Metadata).HasColumnName("metadata");
            entity.Property(e => e.CreatedAt).HasColumnName("created_at");
            entity.Property(e => e.ResumedAt).HasColumnName("resumed_at");
            entity.Property(e => e.Nested).HasColumnName("nested");
            entity.Property(e => e.Decision).HasColumnName("decision");
            entity.Property(e => e.Wait).HasColumnName("wait");
            entity.Property(e => e.WakeAt).HasColumnName("wake_at");
            entity.Property(e => e.GraphId).HasColumnName("graph_id");
            entity.Property(e => e.GraphVersion).HasColumnName("graph_version");
        });
    }
}
