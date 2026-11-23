using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Datalium.Strata.Mcp;

/// <summary>
/// stdio's framing over any two streams (<c>design/50-MCP-Client.md</c>, "stdio"): one JSON-RPC message per
/// <c>\n</c>-terminated line, responses matched to requests by id, so many calls share one connection. A line that
/// isn't JSON, or is bigger than <see cref="McpClientOptions.MaxMessageBytes"/>, closes the connection, since the
/// stream can't be resynchronized.
/// </summary>
internal class LineChannel : McpChannel
{
    private static readonly System.Buffers.SearchValues<byte> Blank = System.Buffers.SearchValues.Create(" \t"u8);

    private readonly Stream _fromServer;
    private readonly Stream _toServer;
    private readonly ConcurrentDictionary<long, TaskCompletionSource<RpcResponse>> _pending = new();
    private readonly SemaphoreSlim _writing = new(1, 1);
    private readonly CancellationTokenSource _stopping = new();
    private Task _reading = Task.CompletedTask;
    private Exception? _closed;

    /// <param name="begin">Whether to start reading now; a subclass that must finish constructing first calls
    /// <see cref="Begin"/> itself.</param>
    public LineChannel(Stream fromServer, Stream toServer, McpClientOptions options, bool begin = true)
        : base(options)
    {
        _fromServer = fromServer;
        _toServer = toServer;
        Endpoint = new McpEndpointInfo("pipe");
        if (begin)
        {
            Begin();
        }
    }

    protected void Begin() => _reading = Task.Run(ReadAsync);

    public override McpEndpointInfo Endpoint { get; }

    public override bool IsOpen => Volatile.Read(ref _closed) is null;

    /// <summary>Why the connection closed, once it has.</summary>
    protected Exception? ClosedReason => Volatile.Read(ref _closed);

    public override async Task<ChannelAnswer> SendAsync(OutgoingRequest request, bool probe, CancellationToken cancellationToken)
    {
        var answer = new TaskCompletionSource<RpcResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[request.Id] = answer;
        try
        {
            if (ClosedReason is { } closed)
            {
                throw Rethrowable(closed);
            }

            await WriteAsync(request.Body, cancellationToken).ConfigureAwait(false);
            return new ChannelAnswer(await answer.Task.WaitAsync(cancellationToken).ConfigureAwait(false));
        }
        finally
        {
            _pending.TryRemove(request.Id, out _);
        }
    }

    public override async Task AbandonAsync(OutgoingRequest request)
    {
        if (!IsOpen)
        {
            return;
        }

        try
        {
            using var bounded = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await WriteAsync(Cancellation(request.Id), bounded.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException or ObjectDisposedException)
        {
            // Fire and forget: the server may already be gone.
        }
    }

    public override Task NotifyAsync(string method, JsonObject? parameters, CancellationToken cancellationToken) =>
        WriteAsync(Notification(method, parameters), cancellationToken);

    public override async ValueTask CloseAsync(Exception reason)
    {
        Fail(reason);
        await StopAsync().ConfigureAwait(false);
        await _stopping.CancelAsync().ConfigureAwait(false);
        await DisposeStreamsAsync().ConfigureAwait(false);
        try
        {
            // Bounded: a process the server started and that outlived it can hold the pipe open.
            await _reading.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // The reader reports its own failures through Fail, and a reader still blocked is abandoned.
        }
    }

    /// <summary>What the subclass does before the streams close (stdio: shuts the process down).</summary>
    protected virtual Task StopAsync() => Task.CompletedTask;

    /// <summary>Why reading stopped at the end of the stream: the server closed its output.</summary>
    protected virtual Exception EndOfStream() => new IOException("the MCP server closed the connection");

    protected async ValueTask DisposeStreamsAsync()
    {
        await TryAsync(() => _toServer.DisposeAsync()).ConfigureAwait(false);
        await TryAsync(() => _fromServer.DisposeAsync()).ConfigureAwait(false);
    }

    /// <summary>Closes the stream to the server, which is a stdio server's signal to exit.</summary>
    protected async ValueTask CloseInputAsync()
    {
        await _writing.WaitAsync().ConfigureAwait(false);
        try
        {
            await TryAsync(() => _toServer.DisposeAsync()).ConfigureAwait(false);
        }
        finally
        {
            _writing.Release();
        }
    }

    protected void Fail(Exception reason)
    {
        if (Interlocked.CompareExchange(ref _closed, reason, null) is not null)
        {
            reason = _closed!;
        }

        foreach (var (id, waiting) in _pending)
        {
            if (_pending.TryRemove(id, out _))
            {
                waiting.TrySetException(Rethrowable(reason));
            }
        }
    }

    private async Task WriteAsync(JsonObject message, CancellationToken cancellationToken)
    {
        var line = Encoding.UTF8.GetBytes(message.ToJsonString() + "\n");
        await _writing.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (ClosedReason is { } closed)
            {
                throw Rethrowable(closed);
            }

            // Not the caller's token: a line half written would corrupt the stream for every other call.
            await _toServer.WriteAsync(line, CancellationToken.None).ConfigureAwait(false);
            await _toServer.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when ((ex is IOException or ObjectDisposedException) && ClosedReason is null)
        {
            var failure = new IOException("the MCP server stopped reading its input", ex);
            Fail(failure);
            throw failure;
        }
        finally
        {
            _writing.Release();
        }
    }

    private async Task ReadAsync()
    {
        var buffer = new byte[64 * 1024];
        var line = new MemoryStream();
        var limit = Options.MaxMessageBytes;
        try
        {
            while (true)
            {
                int read;
                try
                {
                    read = await _fromServer.ReadAsync(buffer, _stopping.Token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is IOException or ObjectDisposedException or OperationCanceledException)
                {
                    Fail(ClosedReason ?? new IOException("the MCP server's output failed", ex));
                    return;
                }

                if (read == 0)
                {
                    Fail(EndOfStream());
                    return;
                }

                var start = 0;
                while (start < read)
                {
                    var newline = Array.IndexOf(buffer, (byte)'\n', start, read - start);
                    var end = newline < 0 ? read : newline;
                    if (line.Length + (end - start) > limit)
                    {
                        Fail(new McpException($"the MCP server sent a message bigger than {limit} bytes"));
                        return;
                    }

                    line.Write(buffer, start, end - start);
                    if (newline < 0)
                    {
                        break;
                    }

                    if (!Dispatch(line.GetBuffer().AsMemory(0, (int)line.Length)))
                    {
                        return;
                    }

                    line.SetLength(0);
                    start = newline + 1;
                }
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Fail(new IOException("reading the MCP server's output failed", ex));
        }
    }

    /// <summary>Handles one line; <see langword="false"/> when it closed the connection.</summary>
    private bool Dispatch(ReadOnlyMemory<byte> line)
    {
        var text = line.Span;
        if (text.Length > 0 && text[^1] == '\r')
        {
            text = text[..^1];
        }

        if (text.IndexOfAnyExcept(Blank) < 0)
        {
            return true;
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(line[..text.Length]);
        }
        catch (JsonException)
        {
            Fail(new IOException("the MCP server wrote a line that isn't JSON to its output"));
            return false;
        }

        using (document)
        {
            var message = document.RootElement;
            if (message.ValueKind != JsonValueKind.Object)
            {
                Report(McpLogLevel.Debug, "ignored a message that isn't a JSON-RPC object");
                return true;
            }

            if (message.TryGetProperty("method", out var method))
            {
                if (message.TryGetProperty("id", out _))
                {
                    var answer = AnswerServerRequest(message);
                    _ = AnswerAsync(answer);
                }
                else
                {
                    Report(McpLogLevel.Debug, $"ignored the notification {method.GetRawText()}");
                }

                return true;
            }

            if (RpcResponse.From(message) is { Id: { } id } response && _pending.TryRemove(id, out var waiting))
            {
                waiting.TrySetResult(response);
            }
            else
            {
                Report(McpLogLevel.Debug, "ignored a response to no request in flight");
            }

            return true;
        }
    }

    private async Task AnswerAsync(JsonObject answer)
    {
        try
        {
            await WriteAsync(answer, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or McpException)
        {
            // The connection's failure is reported to the calls in flight.
        }
    }

    /// <summary>A stored failure as a new exception of the same kind, so each waiting call gets its own stack.</summary>
    internal static Exception Rethrowable(Exception reason) => reason switch
    {
        ObjectDisposedException disposed => new ObjectDisposedException(disposed.ObjectName, disposed.Message),
        McpException mcp => new McpException(mcp.Message, mcp.Code, mcp),
        IOException io => new IOException(io.Message, io),
        _ => new IOException(reason.Message, reason),
    };

    private static async ValueTask TryAsync(Func<ValueTask> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException or NotSupportedException)
        {
            // Closing what is already closed.
        }
    }
}
