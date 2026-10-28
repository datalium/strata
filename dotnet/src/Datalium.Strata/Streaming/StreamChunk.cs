namespace Datalium.Strata.Streaming;

/// <summary>
/// Token streaming (STRATA-SPEC-001). Mirrors <c>rust/strata-core/src/streaming.rs</c>'s
/// <c>StreamChunkType</c>/<c>StreamChunk</c>.
/// </summary>
public enum StreamChunkType
{
    Token,
    NodeStart,
    NodeEnd,
    Error,

    /// <summary>
    /// A fallible node's attempt failed transiently and is about to run again; the payload is the
    /// failed attempt's message. Anything the node streamed during that attempt (a parallel
    /// group's branches included) will be streamed again, so a consumer that has been showing
    /// those tokens should discard them.
    /// </summary>
    Retry,
}

public sealed record StreamChunk(StreamChunkType Type, string NodeName, ReadOnlyMemory<char> Payload);
