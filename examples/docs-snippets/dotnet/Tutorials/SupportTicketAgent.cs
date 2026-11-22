// Samples for https://stratamesh.dev/tutorials/support-ticket-agent/
//
// The tutorial builds Program.cs a step at a time. Each class below is the program as it stands after one step,
// with the samples that step adds, and FullProgram is the program the page ends with. Program.cs is top-level
// code, so here each one is the body of a method that takes the arguments. The tests run them in a directory
// of their own and check what they print against the page. TicketTools is in SupportTicketAgent.TicketTools.cs.

// docs:begin tutorials/support-ticket-agent/program
// docs:begin tutorials/support-ticket-agent/create-project
using System.Text.Json;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Storage.Memory;
using Datalium.Strata.Streaming;
using Datalium.Strata.Tools;
using Microsoft.EntityFrameworkCore;

// docs:hide
using Microsoft.Data.Sqlite;

namespace Datalium.Strata.DocsSnippets.Tutorials.SupportTicketAgent;

internal static class FullProgram
{
    public static async Task RunAsync(string[] args)
    {
        // docs:show
        const string TicketId = "T-1001";
        const string DbPath = "tickets.db";
        // docs:end tutorials/support-ticket-agent/create-project

        var tools = new ToolRegistry().Register(new TicketTools());

        var graph = new StateGraph<Ticket>()
            .AddNode("Classify", async (state, ctx, ct) =>
            {
                var arguments = JsonSerializer.Serialize(new { subject = state.Subject });
                var json = await tools.DispatchToolAsync("classify_ticket", arguments, ToolCallContext.From(ctx), ct);
                var category = JsonDocument.Parse(json).RootElement.GetProperty("result").GetString();
                return state with { Category = category };
            })
            .AddNode("DraftReply", async (state, ctx, ct) =>
            {
                var draft = "";
                foreach (var token in DraftTokens(state.Category!))
                {
                    await Task.Delay(50, ct); // stands in for a model's latency
                    if (ctx.Stream is { } stream)
                    {
                        await stream.EmitTokenAsync(token, ct);
                    }

                    draft += token;
                }

                var now = DateTimeOffset.UtcNow;
                await ctx.History!.AppendAsync(ctx.ThreadId, new ChatMessage(MessageRole.User, state.Subject.AsMemory(), now), ct);
                await ctx.History!.AppendAsync(ctx.ThreadId, new ChatMessage(MessageRole.Assistant, draft.AsMemory(), now), ct);
                return state with { Draft = draft };
            })
            .AddInterruptBoundary("DraftReply")
            .AddNode("SendReply", (state, ctx, _) =>
                ValueTask.FromResult(state with { Status = $"sent (approved by {ctx.Decision!.Reviewer})" }))
            .AddEdge("Classify", "DraftReply")
            .AddEdge("DraftReply", "SendReply")
            .Compile();

        if (args is not ["approve"])
        {
            File.Delete(DbPath); // a new ticket starts from an empty file
        }

        using var checkpoints = new EfCoreCheckpointStore(new CheckpointDbContext(
            new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={DbPath}").Options));
        using var history = new EfCoreHistoryStore(new HistoryDbContext(
            new DbContextOptionsBuilder<HistoryDbContext>().UseSqlite($"Data Source={DbPath}").Options));
        var options = new RunOptions { History = history };

        if (args is ["approve"])
        {
            var checkpoint = await checkpoints.LoadAsync(TicketId);
            var pending = checkpoint!.FrozenState.Deserialize<Ticket>()!;
            Console.WriteLine($"Review {pending.TicketId} ({pending.Category}): {pending.Draft}");

            var resumed = await graph.ResumeAsync(TicketId, HumanSignal.Approve("dana@example.com"), checkpoints, options);
            Console.WriteLine(Describe(resumed));

            // docs:begin tutorials/support-ticket-agent/history
            await foreach (var message in history.GetHistoryAsync(TicketId))
            {
                Console.WriteLine($"[{message.Role}] {message.Content}");
            }
            // docs:end tutorials/support-ticket-agent/history

            return;
        }

        var ticket = new Ticket(TicketId, "My invoice total looks wrong this month");
        var (reader, completion) = graph.RunStreaming(TicketId, ticket, checkpoints, options);
        await foreach (var chunk in reader.ReadAllAsync())
        {
            if (chunk.Type == StreamChunkType.NodeStart)
            {
                Console.WriteLine($"-> {chunk.NodeName}");
            }
            else if (chunk.Type == StreamChunkType.Token)
            {
                Console.Write(chunk.Payload);
            }
        }

        Console.WriteLine();
        Console.WriteLine(Describe(await completion));

        // docs:begin tutorials/support-ticket-agent/draft-tokens
        static IEnumerable<string> DraftTokens(string category)
        {
            var reply = $"Thanks for writing in. Your ticket is with our {category} team, and they will reply within one business day.";
            var words = reply.Split(' ');
            for (var i = 0; i < words.Length; i++)
            {
                yield return i == 0 ? words[i] : " " + words[i];
            }
        }
        // docs:end tutorials/support-ticket-agent/draft-tokens

        static string Describe(GraphExecutionResult<Ticket> result) => result switch
        {
            GraphExecutionResult<Ticket>.Completed done => $"Completed: category {done.State.Category}, status {done.State.Status}",
            GraphExecutionResult<Ticket>.Interrupted paused => $"Paused after {paused.SuspendedNode}, before {paused.NextNode}.",
            GraphExecutionResult<Ticket>.Failed failed => $"Failed: {failed.Message}",
            _ => result.ToString(),
        };

        // docs:hide
    }
}

// docs:show
// docs:begin tutorials/support-ticket-agent/ticket-state
public sealed record Ticket(
    string TicketId,
    string Subject,
    string? Category = null,
    string? Draft = null,
    string Status = "open");
// docs:end tutorials/support-ticket-agent/ticket-state
// docs:end tutorials/support-ticket-agent/program

// Program.cs after step 5: the Classify node, and a streaming run.
internal static class Step5Program
{
    public static async Task RunAsync(string[] args)
    {
        const string TicketId = "T-1001";
        const string DbPath = "tickets.db";

        // docs:begin tutorials/support-ticket-agent/classify-node
        var tools = new ToolRegistry().Register(new TicketTools());

        var graph = new StateGraph<Ticket>()
            .AddNode("Classify", async (state, ctx, ct) =>
            {
                var arguments = JsonSerializer.Serialize(new { subject = state.Subject });
                var json = await tools.DispatchToolAsync("classify_ticket", arguments, ToolCallContext.From(ctx), ct);
                var category = JsonDocument.Parse(json).RootElement.GetProperty("result").GetString();
                return state with { Category = category };
            })
            .Compile();
        // docs:end tutorials/support-ticket-agent/classify-node

        // docs:begin tutorials/support-ticket-agent/run
        if (args is not ["approve"])
        {
            File.Delete(DbPath); // a new ticket starts from an empty file
        }

        using var checkpoints = new EfCoreCheckpointStore(new CheckpointDbContext(
            new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={DbPath}").Options));
        using var history = new EfCoreHistoryStore(new HistoryDbContext(
            new DbContextOptionsBuilder<HistoryDbContext>().UseSqlite($"Data Source={DbPath}").Options));
        var options = new RunOptions { History = history };

        var ticket = new Ticket(TicketId, "My invoice total looks wrong this month");
        var (reader, completion) = graph.RunStreaming(TicketId, ticket, checkpoints, options);
        await foreach (var chunk in reader.ReadAllAsync())
        {
            if (chunk.Type == StreamChunkType.NodeStart)
            {
                Console.WriteLine($"-> {chunk.NodeName}");
            }
            else if (chunk.Type == StreamChunkType.Token)
            {
                Console.Write(chunk.Payload);
            }
        }

        Console.WriteLine();
        Console.WriteLine(Describe(await completion));

        static string Describe(GraphExecutionResult<Ticket> result) => result switch
        {
            GraphExecutionResult<Ticket>.Completed done => $"Completed: category {done.State.Category}, status {done.State.Status}",
            GraphExecutionResult<Ticket>.Interrupted paused => $"Paused after {paused.SuspendedNode}, before {paused.NextNode}.",
            GraphExecutionResult<Ticket>.Failed failed => $"Failed: {failed.Message}",
            _ => result.ToString(),
        };
        // docs:end tutorials/support-ticket-agent/run
    }
}

// Program.cs after step 7: DraftReply, then the interrupt boundary and SendReply.
internal static class Step7Program
{
    public static async Task RunAsync(string[] args)
    {
        const string TicketId = "T-1001";
        const string DbPath = "tickets.db";

        var tools = new ToolRegistry().Register(new TicketTools());

        var graph = new StateGraph<Ticket>()
            .AddNode("Classify", async (state, ctx, ct) =>
            {
                var arguments = JsonSerializer.Serialize(new { subject = state.Subject });
                var json = await tools.DispatchToolAsync("classify_ticket", arguments, ToolCallContext.From(ctx), ct);
                var category = JsonDocument.Parse(json).RootElement.GetProperty("result").GetString();
                return state with { Category = category };
            })
            // docs:begin tutorials/support-ticket-agent/draft-reply-node
            .AddNode("DraftReply", async (state, ctx, ct) =>
            {
                var draft = "";
                foreach (var token in DraftTokens(state.Category!))
                {
                    await Task.Delay(50, ct); // stands in for a model's latency
                    if (ctx.Stream is { } stream)
                    {
                        await stream.EmitTokenAsync(token, ct);
                    }

                    draft += token;
                }

                var now = DateTimeOffset.UtcNow;
                await ctx.History!.AppendAsync(ctx.ThreadId, new ChatMessage(MessageRole.User, state.Subject.AsMemory(), now), ct);
                await ctx.History!.AppendAsync(ctx.ThreadId, new ChatMessage(MessageRole.Assistant, draft.AsMemory(), now), ct);
                return state with { Draft = draft };
            })
            .AddEdge("Classify", "DraftReply")
            // docs:end tutorials/support-ticket-agent/draft-reply-node
            // docs:begin tutorials/support-ticket-agent/pause
            .AddInterruptBoundary("DraftReply")
            .AddNode("SendReply", (state, ctx, _) =>
                ValueTask.FromResult(state with { Status = $"sent (approved by {ctx.Decision!.Reviewer})" }))
            .AddEdge("DraftReply", "SendReply")
            // docs:end tutorials/support-ticket-agent/pause
            .Compile();

        if (args is not ["approve"])
        {
            File.Delete(DbPath); // a new ticket starts from an empty file
        }

        using var checkpoints = new EfCoreCheckpointStore(new CheckpointDbContext(
            new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={DbPath}").Options));
        using var history = new EfCoreHistoryStore(new HistoryDbContext(
            new DbContextOptionsBuilder<HistoryDbContext>().UseSqlite($"Data Source={DbPath}").Options));
        var options = new RunOptions { History = history };

        var ticket = new Ticket(TicketId, "My invoice total looks wrong this month");
        var (reader, completion) = graph.RunStreaming(TicketId, ticket, checkpoints, options);
        await foreach (var chunk in reader.ReadAllAsync())
        {
            if (chunk.Type == StreamChunkType.NodeStart)
            {
                Console.WriteLine($"-> {chunk.NodeName}");
            }
            else if (chunk.Type == StreamChunkType.Token)
            {
                Console.Write(chunk.Payload);
            }
        }

        Console.WriteLine();
        Console.WriteLine(Describe(await completion));

        static IEnumerable<string> DraftTokens(string category)
        {
            var reply = $"Thanks for writing in. Your ticket is with our {category} team, and they will reply within one business day.";
            var words = reply.Split(' ');
            for (var i = 0; i < words.Length; i++)
            {
                yield return i == 0 ? words[i] : " " + words[i];
            }
        }

        static string Describe(GraphExecutionResult<Ticket> result) => result switch
        {
            GraphExecutionResult<Ticket>.Completed done => $"Completed: category {done.State.Category}, status {done.State.Status}",
            GraphExecutionResult<Ticket>.Interrupted paused => $"Paused after {paused.SuspendedNode}, before {paused.NextNode}.",
            GraphExecutionResult<Ticket>.Failed failed => $"Failed: {failed.Message}",
            _ => result.ToString(),
        };
    }
}

// Program.cs after step 8: an approve argument resumes the ticket.
internal static class Step8Program
{
    public static async Task RunAsync(string[] args)
    {
        const string TicketId = "T-1001";
        const string DbPath = "tickets.db";

        var tools = new ToolRegistry().Register(new TicketTools());

        var graph = new StateGraph<Ticket>()
            .AddNode("Classify", async (state, ctx, ct) =>
            {
                var arguments = JsonSerializer.Serialize(new { subject = state.Subject });
                var json = await tools.DispatchToolAsync("classify_ticket", arguments, ToolCallContext.From(ctx), ct);
                var category = JsonDocument.Parse(json).RootElement.GetProperty("result").GetString();
                return state with { Category = category };
            })
            .AddNode("DraftReply", async (state, ctx, ct) =>
            {
                var draft = "";
                foreach (var token in DraftTokens(state.Category!))
                {
                    await Task.Delay(50, ct); // stands in for a model's latency
                    if (ctx.Stream is { } stream)
                    {
                        await stream.EmitTokenAsync(token, ct);
                    }

                    draft += token;
                }

                var now = DateTimeOffset.UtcNow;
                await ctx.History!.AppendAsync(ctx.ThreadId, new ChatMessage(MessageRole.User, state.Subject.AsMemory(), now), ct);
                await ctx.History!.AppendAsync(ctx.ThreadId, new ChatMessage(MessageRole.Assistant, draft.AsMemory(), now), ct);
                return state with { Draft = draft };
            })
            .AddEdge("Classify", "DraftReply")
            .AddInterruptBoundary("DraftReply")
            .AddNode("SendReply", (state, ctx, _) =>
                ValueTask.FromResult(state with { Status = $"sent (approved by {ctx.Decision!.Reviewer})" }))
            .AddEdge("DraftReply", "SendReply")
            .Compile();

        if (args is not ["approve"])
        {
            File.Delete(DbPath); // a new ticket starts from an empty file
        }

        using var checkpoints = new EfCoreCheckpointStore(new CheckpointDbContext(
            new DbContextOptionsBuilder<CheckpointDbContext>().UseSqlite($"Data Source={DbPath}").Options));
        using var history = new EfCoreHistoryStore(new HistoryDbContext(
            new DbContextOptionsBuilder<HistoryDbContext>().UseSqlite($"Data Source={DbPath}").Options));
        var options = new RunOptions { History = history };

        // docs:begin tutorials/support-ticket-agent/approve
        if (args is ["approve"])
        {
            var checkpoint = await checkpoints.LoadAsync(TicketId);
            var pending = checkpoint!.FrozenState.Deserialize<Ticket>()!;
            Console.WriteLine($"Review {pending.TicketId} ({pending.Category}): {pending.Draft}");

            var resumed = await graph.ResumeAsync(TicketId, HumanSignal.Approve("dana@example.com"), checkpoints, options);
            Console.WriteLine(Describe(resumed));

            return;
        }
        // docs:end tutorials/support-ticket-agent/approve

        var ticket = new Ticket(TicketId, "My invoice total looks wrong this month");
        var (reader, completion) = graph.RunStreaming(TicketId, ticket, checkpoints, options);
        await foreach (var chunk in reader.ReadAllAsync())
        {
            if (chunk.Type == StreamChunkType.NodeStart)
            {
                Console.WriteLine($"-> {chunk.NodeName}");
            }
            else if (chunk.Type == StreamChunkType.Token)
            {
                Console.Write(chunk.Payload);
            }
        }

        Console.WriteLine();
        Console.WriteLine(Describe(await completion));

        static IEnumerable<string> DraftTokens(string category)
        {
            var reply = $"Thanks for writing in. Your ticket is with our {category} team, and they will reply within one business day.";
            var words = reply.Split(' ');
            for (var i = 0; i < words.Length; i++)
            {
                yield return i == 0 ? words[i] : " " + words[i];
            }
        }

        static string Describe(GraphExecutionResult<Ticket> result) => result switch
        {
            GraphExecutionResult<Ticket>.Completed done => $"Completed: category {done.State.Category}, status {done.State.Status}",
            GraphExecutionResult<Ticket>.Interrupted paused => $"Paused after {paused.SuspendedNode}, before {paused.NextNode}.",
            GraphExecutionResult<Ticket>.Failed failed => $"Failed: {failed.Message}",
            _ => result.ToString(),
        };
    }
}

[Collection(SupportTicketAgentConsole.Name)]
public class SupportTicketAgentSamples
{
    private const string Reply =
        "Thanks for writing in. Your ticket is with our billing team, and they will reply within one business day.";

    private const string FirstRun = $"-> Classify\n-> DraftReply\n{Reply}\nPaused after DraftReply, before SendReply.";

    private const string Approval =
        $"Review T-1001 (billing): {Reply}\nCompleted: category billing, status sent (approved by dana@example.com)";

    [Fact]
    public async Task Step5ClassifiesTheTicket()
    {
        using var directory = new ProgramDirectory();
        Assert.Equal("-> Classify\n\nCompleted: category billing, status open", await directory.RunAsync(Step5Program.RunAsync));
    }

    [Fact]
    public async Task Step7PausesAfterTheDraft()
    {
        using var directory = new ProgramDirectory();
        Assert.Equal(FirstRun, await directory.RunAsync(Step7Program.RunAsync));
    }

    [Fact]
    public async Task Step8ApprovesAfterARestart()
    {
        using var directory = new ProgramDirectory();
        Assert.Equal(FirstRun, await directory.RunAsync(Step8Program.RunAsync));
        Assert.Equal(Approval, await directory.RunAsync(Step8Program.RunAsync, "approve"));

        // A waiting thread can be answered once: the draft still shows, and the resume is refused.
        Assert.Equal(
            $"Review T-1001 (billing): {Reply}\nFailed: checkpoint for thread 'T-1001' was already resumed",
            await directory.RunAsync(Step8Program.RunAsync, "approve"));
    }

    [Fact]
    public async Task TheFullProgramPrintsWhatThePageShows()
    {
        using var directory = new ProgramDirectory();
        Assert.Equal(FirstRun, await directory.RunAsync(FullProgram.RunAsync));
        Assert.Equal(
            $"{Approval}\n[User] My invoice total looks wrong this month\n[Assistant] {Reply}",
            await directory.RunAsync(FullProgram.RunAsync, "approve"));
    }

    // A directory for the program's tickets.db, made the current directory while the test runs, and the console
    // output of each run. The collection runs alone, so changing both is safe.
    private sealed class ProgramDirectory : IDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"strata-docs-ticket-agent-{Guid.NewGuid():N}");
        private readonly string _previous = Directory.GetCurrentDirectory();

        public ProgramDirectory()
        {
            Directory.CreateDirectory(_path);
            Directory.SetCurrentDirectory(_path);
        }

        public async Task<string> RunAsync(Func<string[], Task> program, params string[] args)
        {
            var output = new StringWriter();
            var console = Console.Out;
            Console.SetOut(output);
            try
            {
                await program(args);
            }
            finally
            {
                Console.SetOut(console);
            }

            return output.ToString().ReplaceLineEndings("\n").Trim();
        }

        public void Dispose()
        {
            Directory.SetCurrentDirectory(_previous);
            SqliteConnection.ClearAllPools();
            try
            {
                Directory.Delete(_path, recursive: true);
            }
            catch (IOException)
            {
                // Left for the OS to clean up.
            }
        }
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SupportTicketAgentConsole
{
    public const string Name = "Support ticket agent console";
}
