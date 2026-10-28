namespace Datalium.Strata.Memory;

/// <summary>
/// Per CLAUDE.md item 6: "Graph nodes access history via <c>IExecutionContext</c> handles,
/// never by carrying full chat history in state payloads." Every async node handler receives one:
/// <see cref="Graph.NodeContext"/> implements this interface, with the history store passed in
/// <see cref="Graph.RunOptions.History"/> (<c>design/17-Async-Execution.md</c>). Sync handlers don't
/// get a context; <see cref="StrataExecutionContext"/> remains a standalone handle a caller can
/// construct and pass to tool/history code itself.
/// </summary>
public interface IExecutionContext
{
    string ThreadId { get; }

    IHistoryStore History { get; }
}
