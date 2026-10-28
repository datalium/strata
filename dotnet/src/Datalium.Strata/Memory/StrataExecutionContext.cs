namespace Datalium.Strata.Memory;

/// <summary>
/// Default <see cref="IExecutionContext"/> implementation. Named <c>StrataExecutionContext</c>
/// rather than the bare <c>ExecutionContext</c> the Rust side uses (<c>datalium_strata::ExecutionContext</c>)
/// to avoid colliding with <see cref="System.Threading.ExecutionContext"/> — a real BCL type
/// that becomes ambiguous the moment a consumer's file also has a <c>using System.Threading;</c>.
/// </summary>
public sealed class StrataExecutionContext(string threadId, IHistoryStore history) : IExecutionContext
{
    public string ThreadId { get; } = threadId;

    public IHistoryStore History { get; } = history;
}
