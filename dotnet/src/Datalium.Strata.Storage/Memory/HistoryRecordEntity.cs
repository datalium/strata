namespace Datalium.Strata.Storage.Memory;

/// <summary>EF Core entity backing <see cref="EfCoreHistoryStore"/>'s SQLite table.</summary>
public sealed class HistoryRecordEntity
{
    public required string SessionId { get; init; }

    public required long Sequence { get; init; }

    public required string Role { get; init; }

    public required string Content { get; init; }

    public required string Timestamp { get; init; }
}
