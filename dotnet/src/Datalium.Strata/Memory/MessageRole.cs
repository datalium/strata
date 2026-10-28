namespace Datalium.Strata.Memory;

/// <summary>
/// The runtime chat-role enum for <see cref="ChatMessage"/> (design/03-Architecture.md).
/// Distinct from <c>Datalium.Strata.Storage.Schema.MessageRole</c> (the Phase 1 interop wire
/// enum) — same core-type/interop-type split as <c>Datalium.Strata.Interruption.Checkpoint</c>
/// vs. <c>Datalium.Strata.Storage.Schema.Checkpoint</c>.
/// </summary>
public enum MessageRole
{
    System,
    User,
    Assistant,
    Tool,
}
