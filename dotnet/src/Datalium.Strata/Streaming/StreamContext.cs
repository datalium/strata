using System.Threading.Channels;

namespace Datalium.Strata.Streaming;

/// <summary>
/// Decouples the graph state pipeline from output delivery: a node writes chunks to a bounded
/// <see cref="Channel{T}"/>, and a consumer reads them independently. The channel is bounded at
/// <see cref="StreamContext.DefaultChannelCapacity"/> (1024) chunks; once full, <see cref="EmitTokenAsync"/>'s
/// await pauses the node rather than dropping chunks — this is ordinary bounded-channel
/// backpressure (<see cref="BoundedChannelFullMode.Wait"/>), not a custom mechanism.
///
/// The engine hands one to every node of a streaming run (<c>CompiledGraph{TState}.RunStreaming</c>):
/// a sync streaming handler gets it directly and may use the synchronous emits, an async handler finds
/// it on <c>NodeContext.Stream</c> and should await the async ones, passing its token.
/// </summary>
public interface IStreamContext
{
    ValueTask EmitTokenAsync(string token, CancellationToken cancellationToken = default);

    ValueTask EmitDiagnosticAsync(StreamChunkType type, string message, CancellationToken cancellationToken = default);

    /// <summary>Synchronous counterpart to <see cref="EmitTokenAsync"/>, for a streaming node's synchronous handler.</summary>
    void EmitToken(string token);

    /// <summary>Synchronous counterpart to <see cref="EmitDiagnosticAsync"/>, for a streaming node's synchronous handler.</summary>
    void EmitDiagnostic(StreamChunkType type, string message);
}

/// <summary>
/// A streaming node handler: like a plain <c>Func&lt;TState,TState&gt;</c> node, but it also
/// receives an <see cref="IStreamContext"/> scoped to itself, so it can push
/// <see cref="StreamChunk"/>s (e.g. LLM response tokens) while it runs instead of only returning
/// a final state. Added via <c>StateGraph{TState}.AddStreamingNode</c>; only meaningful under
/// <c>CompiledGraph{TState}.RunStreaming</c> — running a graph that contains one via the plain
/// <c>RunAsync</c> fails at that node, since there's no <see cref="IStreamContext"/> to hand it.
/// </summary>
public delegate TState StreamingNodeHandler<TState>(TState state, IStreamContext context);

public sealed class StreamContext : IStreamContext
{
    public const int DefaultChannelCapacity = 1024;

    private readonly ChannelWriter<StreamChunk> _writer;
    private readonly string _nodeName;

    // Set on the context a subgraph node hands its child (see `Scoped`): the qualified name of that
    // node, prefixed to every node name the child derives with `ForNode`.
    private readonly string? _scope;

    private StreamContext(ChannelWriter<StreamChunk> writer, string nodeName, string? scope = null)
    {
        _writer = writer;
        _nodeName = nodeName;
        _scope = scope;
    }

    /// <summary>Creates a context/reader pair bounded at <see cref="DefaultChannelCapacity"/>.</summary>
    public static (StreamContext Context, ChannelReader<StreamChunk> Reader) CreateChannel(string nodeName) =>
        CreateChannel(nodeName, DefaultChannelCapacity);

    public static (StreamContext Context, ChannelReader<StreamChunk> Reader) CreateChannel(string nodeName, int capacity)
    {
        var channel = Channel.CreateBounded<StreamChunk>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            // Not single-writer: the branches of a parallel group each write from their own thread.
            SingleWriter = false,
        });

        return (new StreamContext(channel.Writer, nodeName), channel.Reader);
    }

    /// <summary>
    /// Returns a copy of this context retargeted at <paramref name="nodeName"/>, sharing the same
    /// underlying channel. This is how the engine (see
    /// <c>CompiledGraph{TState}.ExecuteFromAsync</c>) gives every node its own <see cref="StreamContext"/>
    /// — with the right node name on each emitted chunk — out of a single channel created once per
    /// run. Mirrors <c>streaming.rs</c>'s <c>StreamContext::for_node</c>.
    ///
    /// Inside a subgraph (see <see cref="Scoped"/>) the name is qualified with the subgraph node's
    /// own: <c>"&lt;subgraph&gt;/&lt;nodeName&gt;"</c>.
    /// </summary>
    public StreamContext ForNode(string nodeName) =>
        new(_writer, _scope is null ? nodeName : $"{_scope}/{nodeName}", _scope);

    /// <summary>
    /// The context a subgraph node hands its child graph: same channel, but every node the child
    /// derives with <see cref="ForNode"/> is named under this context's node (<c>"Research/Draft"</c>,
    /// then <c>"Research/Deep/Draft"</c> for a grandchild), so the child's chunks say which subgraph
    /// they came from.
    /// </summary>
    internal StreamContext Scoped() => new(_writer, _nodeName, _nodeName);

    public ValueTask EmitTokenAsync(string token, CancellationToken cancellationToken = default) =>
        EmitAsync(StreamChunkType.Token, token, cancellationToken);

    public ValueTask EmitDiagnosticAsync(StreamChunkType type, string message, CancellationToken cancellationToken = default) =>
        EmitAsync(type, message, cancellationToken);

    private ValueTask EmitAsync(StreamChunkType type, string payload, CancellationToken cancellationToken)
    {
        var chunk = new StreamChunk(type, _nodeName, payload.AsMemory());
        var write = _writer.WriteAsync(chunk, cancellationToken);
        return write.IsCompletedSuccessfully ? write : Settle(write, cancellationToken);
    }

    /// <summary>
    /// A write still waiting when its token fires can lose the race to the run ending: the engine stops waiting for the
    /// handler, the run returns and the channel is completed, so the write fails with
    /// <see cref="ChannelClosedException"/>. The handler was cancelled either way, so it sees that
    /// (<see cref="OperationCanceledException"/>), whichever came first.
    /// </summary>
    private static async ValueTask Settle(ValueTask write, CancellationToken cancellationToken)
    {
        try
        {
            await write.ConfigureAwait(false);
        }
        catch (ChannelClosedException closed) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(closed.Message, closed, cancellationToken);
        }
    }

    /// <summary>
    /// The engine's own chunk (<c>NodeEnd</c>, <c>Retry</c>, <c>Error</c>): written at once when there is room, else
    /// waited for until <paramref name="cancellationToken"/> fires, when it is dropped. A run that is ending doesn't wait
    /// for a reader that may have stopped reading.
    /// </summary>
    internal async ValueTask EmitLifecycleAsync(StreamChunkType type, string message, CancellationToken cancellationToken)
    {
        var chunk = new StreamChunk(type, _nodeName, message.AsMemory());
        if (_writer.TryWrite(chunk))
        {
            return;
        }

        try
        {
            await _writer.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    /// <summary>
    /// Synchronous counterparts to <see cref="EmitTokenAsync"/>/<see cref="EmitDiagnosticAsync"/>,
    /// for use from inside a streaming node's synchronous handler
    /// (<see cref="StreamingNodeHandler{TState}"/>). Safe to call from any thread — unlike Rust's
    /// `blocking_send`, a bounded <see cref="Channel{T}"/> write has no runtime-thread restriction
    /// to worry about — and the engine runs a sync handler on a thread-pool thread (or, for a
    /// parallel branch, a thread of its own), never on the executor or the caller, so a blocking
    /// wait here never blocks anything the run needs. An async handler uses the async emits.
    /// </summary>
    public void EmitToken(string token) => EmitBlocking(StreamChunkType.Token, token);

    public void EmitDiagnostic(StreamChunkType type, string message) => EmitBlocking(type, message);

    private void EmitBlocking(StreamChunkType type, string payload)
    {
        var chunk = new StreamChunk(type, _nodeName, payload.AsMemory());
        _writer.WriteAsync(chunk).AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Signals no more chunks are coming, so a reader's <c>ReadAllAsync</c> completes instead of
    /// waiting forever. Rust's equivalent primitive needs no such method — dropping every
    /// <c>Sender</c> clone closes the channel automatically — but C#'s <see cref="Channel{T}"/>
    /// has no drop-based equivalent, so an explicit completion signal is this type's
    /// C#-idiomatic counterpart.
    /// </summary>
    public void Complete() => _writer.Complete();
}
