using System.Runtime.CompilerServices;
using System.Text;

namespace Datalium.Strata.Mcp;

/// <summary>One server-sent event: its type, its data, and the last event id seen so far.</summary>
internal sealed record SseEvent(string EventType, string Data, string? LastEventId);

/// <summary>
/// Reading <c>text/event-stream</c> by the WHATWG rules (<c>schema/fixtures/mcp/sse.json</c>), as Rust's
/// <c>datalium-strata-mcp</c> reads it. Not the shared framework's <c>SseParser</c>, which keeps an event's type for the
/// next event when the event had no data, and reports only the id set in each event, where the rules reset the type
/// and carry the last id over.
/// </summary>
internal static class McpSse
{
    public static async IAsyncEnumerable<SseEvent> ReadAsync(Stream stream, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var reader = new StreamReader(stream, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: false);
        var data = new StringBuilder();
        string? type = null;
        string? lastId = null;
        var first = true;
        await foreach (var raw in LinesAsync(reader, cancellationToken).ConfigureAwait(false))
        {
            var line = first && raw.StartsWith('﻿') ? raw[1..] : raw;
            first = false;
            if (line.Length == 0)
            {
                if (data.Length > 0)
                {
                    yield return new SseEvent(type ?? "message", data.ToString(0, data.Length - 1), lastId);
                }

                data.Clear();
                type = null;
                continue;
            }

            if (line[0] == ':')
            {
                continue;
            }

            var colon = line.IndexOf(':', StringComparison.Ordinal);
            var field = colon < 0 ? line : line[..colon];
            var value = colon < 0 ? string.Empty : line[(colon + 1)..];
            if (value.StartsWith(' '))
            {
                value = value[1..];
            }

            switch (field)
            {
                case "data":
                    data.Append(value).Append('\n');
                    break;
                case "event":
                    type = value;
                    break;
                case "id" when !value.Contains('\0', StringComparison.Ordinal):
                    lastId = value;
                    break;
            }
        }
    }

    /// <summary>The stream's lines, each ended by <c>\r\n</c>, <c>\n</c> or <c>\r</c>; an unfinished last line is
    /// dropped.</summary>
    private static async IAsyncEnumerable<string> LinesAsync(StreamReader reader, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new char[4096];
        var line = new StringBuilder();
        var afterCr = false;
        int read;
        while ((read = await reader.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
        {
            for (var i = 0; i < read; i++)
            {
                var ch = buffer[i];
                if (afterCr)
                {
                    afterCr = false;
                    if (ch == '\n')
                    {
                        continue;
                    }
                }

                if (ch is '\r' or '\n')
                {
                    afterCr = ch == '\r';
                    yield return line.ToString();
                    line.Clear();
                }
                else
                {
                    line.Append(ch);
                }
            }
        }
    }
}
