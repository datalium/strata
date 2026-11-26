using System.Diagnostics;
using Datalium.Strata.McpFixture;

// The stdio fixture server (design/50-MCP-Client.md, "Tests"). Its script comes from its arguments (FixtureScript.Parse),
// plus what only a process can do:
//   --sleep               be the child of another fixture server: sleep until killed
//   --spawn-child         start a child of its own (--sleep), for the test that stops the whole process tree
//   --ignore-stdin-close  don't exit when stdin closes, so the client must stop it
//   --stderr <text>       write a line to stderr first
if (args.Contains("--sleep"))
{
    await Task.Delay(Timeout.Infinite);
    return 0;
}

var server = new FixtureServer(FixtureScript.Parse(args));
for (var i = 0; i < args.Length - 1; i++)
{
    if (args[i] == "--stderr")
    {
        await Console.Error.WriteLineAsync(args[i + 1]);
        await Console.Error.FlushAsync();
    }
}

if (args.Contains("--spawn-child"))
{
    var self = Environment.ProcessPath!;
    var start = new ProcessStartInfo(self) { UseShellExecute = false, RedirectStandardInput = true };
    if (Path.GetFileNameWithoutExtension(self).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
    {
        start.ArgumentList.Add(typeof(FixtureServer).Assembly.Location);
    }

    start.ArgumentList.Add("--sleep");
    server.ChildProcessId = Process.Start(start)!.Id;
}

await server.RunLinesAsync(Console.OpenStandardInput(), Console.OpenStandardOutput(), () => Environment.Exit(3));
if (args.Contains("--ignore-stdin-close"))
{
    await Task.Delay(Timeout.Infinite);
}

return 0;
