using System.Diagnostics;
using System.Text;

namespace Datalium.Strata.Mcp;

/// <summary>
/// A server Strata runs as a child process (<c>design/50-MCP-Client.md</c>, "stdio"): line-framed messages on its
/// stdin and stdout, its stderr read all the time and reported to the log, and the spec's shutdown: close stdin,
/// wait, then stop the process and the processes it started.
/// </summary>
internal sealed class StdioChannel : LineChannel
{
    private readonly Process _process;
    private readonly TimeSpan _shutdownTimeout;
    private readonly Task _stderr;

    private StdioChannel(Process process, StdioMcpServer server, McpClientOptions options)
        : base(process.StandardOutput.BaseStream, process.StandardInput.BaseStream, options, begin: false)
    {
        _process = process;
        _shutdownTimeout = server.ShutdownTimeout;
        _stderr = Task.Run(DrainStderrAsync);
        Begin();
    }

    /// <summary>The server's process id, for the tests that check it stops.</summary>
    internal int ProcessId => _process.Id;

    public static StdioChannel Start(StdioMcpServer server, McpClientOptions options)
    {
        var start = StdioCommand.StartInfo(server);
        Process process;
        try
        {
            process = Process.Start(start) ?? throw new IOException($"the MCP server '{server.Command}' didn't start");
        }
        catch (System.ComponentModel.Win32Exception ex)
        {
            throw new IOException($"the MCP server '{server.Command}' didn't start: {ex.Message}", ex);
        }

        return new StdioChannel(process, server, options);
    }

    protected override Exception EndOfStream()
    {
        try
        {
            if (_process.WaitForExit(TimeSpan.FromSeconds(1)))
            {
                return new IOException($"the MCP server exited with code {_process.ExitCode}");
            }
        }
        catch (InvalidOperationException)
        {
            // Not started, or already disposed.
        }

        return new IOException("the MCP server closed its output");
    }

    protected override async Task StopAsync()
    {
        await CloseInputAsync().ConfigureAwait(false);
        try
        {
            using var wait = new CancellationTokenSource(_shutdownTimeout);
            await _process.WaitForExitAsync(wait.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Stop();
        }
        catch (InvalidOperationException)
        {
            // Already gone.
        }

        try
        {
            await _stderr.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // A grandchild may hold stderr open; it is stopped or orphaned, and the pipe closes with the process.
        }

        _process.Dispose();
    }

    /// <summary>Stops the server and every process it started: <c>npx</c> runs the real server as its child.</summary>
    private void Stop()
    {
        try
        {
            _process.Kill(entireProcessTree: true);
            _process.WaitForExit(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception or NotSupportedException)
        {
            // It exited meanwhile.
        }
    }

    private async Task DrainStderrAsync()
    {
        try
        {
            var reader = _process.StandardError;
            while (await reader.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                // Debug only, and never on a span: a server's stderr can hold secrets.
                Report(McpLogLevel.Debug, "stderr: " + line);
            }
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
        {
            // The process is gone.
        }
    }
}

/// <summary>
/// How a stdio server's command becomes a process (<c>design/50-MCP-Client.md</c>, "stdio", "Windows"): its
/// environment, and on Windows the <c>PATH</c> / <c>PATHEXT</c> search a shell does, with a batch file run through
/// <c>cmd.exe</c> and its arguments quoted for it, as Rust's standard library quotes them.
/// </summary>
internal static class StdioCommand
{
    public static ProcessStartInfo StartInfo(StdioMcpServer server)
    {
        var start = new ProcessStartInfo
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
            StandardInputEncoding = new UTF8Encoding(false),
            WorkingDirectory = server.WorkingDirectory ?? string.Empty,
        };

        if (!server.InheritEnvironment)
        {
            var keep = OperatingSystem.IsWindows() ? new[] { "PATH", "SystemRoot" } : ["PATH"];
            var kept = keep.ToDictionary(k => k, System.Environment.GetEnvironmentVariable, StringComparer.OrdinalIgnoreCase);
            start.Environment.Clear();
            foreach (var (name, value) in kept)
            {
                if (value is not null)
                {
                    start.Environment[name] = value;
                }
            }
        }

        foreach (var (name, value) in server.Environment)
        {
            if (value is null)
            {
                start.Environment.Remove(name);
            }
            else
            {
                start.Environment[name] = value;
            }
        }

        var program = OperatingSystem.IsWindows() ? Resolve(server.Command, Variable(start, "PATH"), Variable(start, "PATHEXT")) : server.Command;
        if (OperatingSystem.IsWindows() && IsBatchFile(program))
        {
            start.FileName = Variable(start, "ComSpec") ?? Path.Combine(System.Environment.SystemDirectory, "cmd.exe");
            start.Arguments = BatchArguments(program, server.Arguments);
        }
        else
        {
            start.FileName = program;
            foreach (var argument in server.Arguments)
            {
                start.ArgumentList.Add(argument);
            }
        }

        return start;
    }

    /// <summary>
    /// <paramref name="command"/> found as a shell finds it: a name with a directory or an extension is used as it is
    /// (an extensionless name in a directory gets <c>PATHEXT</c> tried too); a bare name is looked for in each
    /// <c>PATH</c> directory, with each <c>PATHEXT</c> extension. Not found, it is returned unchanged, and starting it
    /// fails with the OS's message.
    /// </summary>
    internal static string Resolve(string command, string? path, string? pathext)
    {
        var extensions = (pathext ?? ".COM;.EXE;.BAT;.CMD").Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var hasDirectory = command.Contains('\\') || command.Contains('/');
        var hasExtension = Path.HasExtension(command);
        if (hasDirectory)
        {
            return hasExtension || File.Exists(command)
                ? command
                : extensions.Select(e => command + e).FirstOrDefault(File.Exists) ?? command;
        }

        foreach (var directory in (path ?? string.Empty).Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var candidate = Path.Combine(directory.Trim('"'), command);
            if (hasExtension && File.Exists(candidate))
            {
                return candidate;
            }

            if (!hasExtension)
            {
                foreach (var extension in extensions)
                {
                    if (File.Exists(candidate + extension))
                    {
                        return candidate + extension;
                    }
                }
            }
        }

        return command;
    }

    internal static bool IsBatchFile(string program) =>
        program.EndsWith(".cmd", StringComparison.OrdinalIgnoreCase) || program.EndsWith(".bat", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// <c>cmd.exe</c>'s arguments to run <paramref name="script"/> with <paramref name="arguments"/>: Rust's standard
    /// library's scheme (<c>make_bat_command_line</c>, the fix for CVE-2024-24576). Command extensions on and delayed
    /// expansion off; the whole line in one more pair of quotes; an argument quoted unless every character is known
    /// to be safe, its <c>"</c> doubled, and its <c>%</c> written <c>%%cd:~,%</c> so no variable expands. An argument
    /// holding a line break is refused, since cmd would cut the line there.
    /// </summary>
    internal static string BatchArguments(string script, IEnumerable<string> arguments)
    {
        if (script.Contains('"') || script.EndsWith('\\'))
        {
            throw new ArgumentException($"the batch file '{script}' can't be run: its name has a '\"' or ends with '\\'");
        }

        var line = new StringBuilder("/e:ON /v:OFF /d /c \"\"").Append(script).Append('"');
        foreach (var argument in arguments)
        {
            if (argument.Contains('\r') || argument.Contains('\n') || argument.Contains('\0'))
            {
                throw new ArgumentException("an argument to a batch file can't hold a line break or a NUL");
            }

            line.Append(' ');
            AppendBatchArgument(line, argument);
        }

        return line.Append('"').ToString();
    }

    private static void AppendBatchArgument(StringBuilder line, string argument)
    {
        const string Unquoted = @"#$*+-./:?@\_";
        var quote = argument.Length == 0 || argument.EndsWith('\\')
            || argument.Any(ch => (char.IsAscii(ch) && !char.IsAsciiLetterOrDigit(ch) && !Unquoted.Contains(ch)) || char.IsControl(ch));
        if (quote)
        {
            line.Append('"');
        }

        var backslashes = 0;
        foreach (var ch in argument)
        {
            if (ch == '\\')
            {
                backslashes++;
            }
            else
            {
                if (ch == '"')
                {
                    line.Append('\\', backslashes).Append('"');
                }
                else if (ch == '%')
                {
                    line.Append("%%cd:~,");
                }

                backslashes = 0;
            }

            line.Append(ch);
        }

        if (quote)
        {
            line.Append('\\', backslashes).Append('"');
        }
    }

    private static string? Variable(ProcessStartInfo start, string name) =>
        start.Environment.TryGetValue(name, out var value) ? value : System.Environment.GetEnvironmentVariable(name);
}
