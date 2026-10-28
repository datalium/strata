namespace Datalium.Strata.Example.DotnetAgent;

public sealed record TicketState
{
    public required string TicketId { get; init; }

    public required string Subject { get; init; }

    public string? Category { get; init; }

    public string? ResolvedBy { get; init; }

    public string? Resolution { get; init; }
}
