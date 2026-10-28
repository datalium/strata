using System.Text.Json;
using Datalium.Strata.Graph;
using Datalium.Strata.Hosting;
using Datalium.Strata.Interruption;
using Datalium.Strata.Memory;
using Datalium.Strata.Streaming;
using Datalium.Strata.Tools;
using Microsoft.Extensions.Logging;

namespace Datalium.Strata.Example.DotnetAgent;

/// <summary>
/// Minimal example agent demonstrating Datalium.Strata's primitives working together in the .NET Generic Host: a
/// <see cref="StateGraph{TState}"/> whose entry node calls a <c>[StrataTool]</c>-generated tool, streams progress
/// tokens, and records to history, then pauses at a human-in-the-loop interrupt boundary before a human approves and
/// it resumes to completion. Mirrors <c>examples/rust-agent</c>'s identical support-ticket-triage scenario.
///
/// The host wires it (<c>design/35-Hosting-Integration.md</c>): <see cref="AddSupportTickets"/> registers the
/// checkpoint and history stores and the compiled graph with <c>AddStrata()</c>, and a run goes through the
/// <see cref="StrataRunner{TState}"/> the host gives, whose run options carry the container's history store.
/// <c>ClassifyTicket</c> is an async node (<c>design/17-Async-Execution.md</c>): it awaits the tool dispatch, the
/// run's stream and the history store directly, passing its token on, and reaches the stream and the history through
/// its <see cref="NodeContext"/>. <c>ResolveTicket</c> is an ordinary sync node; the two mix freely.
/// </summary>
public static class SupportTicketDemo
{
    public const string ClassifyNode = "ClassifyTicket";

    /// <summary>
    /// Registers the example with the host: in-memory checkpoint and history stores, the ticket tools, and the
    /// compiled graph, which <c>AddGraph</c> pairs with a <see cref="StrataRunner{TState}"/>. A real service would
    /// swap the stores for <c>AddSqliteCheckpoints()</c>, <c>AddPostgresCheckpoints()</c> and so on, configured from
    /// the <c>Strata</c> section.
    /// </summary>
    public static IStrataBuilder AddSupportTickets(this IServiceCollection services)
    {
        services.AddSingleton<TicketTools>();
        return services.AddStrata()
            .AddInMemoryCheckpoints()
            .AddInMemoryHistory()
            .AddGraph(provider => BuildGraph(provider.GetRequiredService<TicketTools>()));
    }

    /// <summary>The graph: classify (a tool call, streamed progress, a history entry), pause for approval, resolve.</summary>
    public static CompiledGraph<TicketState> BuildGraph(TicketTools tools) =>
        new StateGraph<TicketState>()
            .AddNode(
                ClassifyNode,
                async (state, context, ct) =>
                {
                    // Under RunStreaming the engine hands every node the run's stream; under RunAsync there is none.
                    if (context.Stream is { } stream)
                    {
                        await stream.EmitTokenAsync("Classifying ticket", ct);
                        await stream.EmitTokenAsync("...", ct);
                    }

                    var argsJson = JsonSerializer.Serialize(new { subject = state.Subject });
                    var resultJson = await tools.DispatchToolAsync("classify_ticket", argsJson, ToolCallContext.From(context), ct);
                    var category = JsonDocument.Parse(resultJson).RootElement.GetProperty("result").GetString()!;

                    await context.History!.AppendAsync(
                        context.ThreadId,
                        new ChatMessage(MessageRole.Assistant, $"Classified as '{category}'.".AsMemory(), DateTimeOffset.UtcNow),
                        ct);

                    return state with { Category = category };
                })
            .AddInterruptBoundary(ClassifyNode)
            .AddNode(
                "ResolveTicket",
                state => state with
                {
                    Resolution =
                        $"Escalated to the {state.Category ?? "general"} team (approved by {state.ResolvedBy ?? "unknown"}).",
                })
            .AddEdge(ClassifyNode, "ResolveTicket")
            .Compile();

    /// <summary>Runs one ticket to the boundary, approves it, resumes it to the end, and reads its history back.</summary>
    public static async Task<(TicketState FinalState, IReadOnlyList<ChatMessage> History)> RunAsync(
        StrataRunner<TicketState> runner,
        IHistoryStore historyStore,
        ILogger logger,
        CancellationToken ct = default)
    {
        const string ticketId = "T-1001";
        var initial = new TicketState
        {
            TicketId = ticketId,
            Subject = "My invoice total looks wrong this month",
        };

        // ---- Run to the interrupt boundary (Freeze/Flush), printing the node's tokens as they come ----
        logger.LogInformation("Running ClassifyTicket...");
        var (reader, completion) = runner.Graph.RunStreaming(ticketId, initial, runner.Store, runner.CreateOptions(), ct);
        await foreach (var chunk in reader.ReadAllAsync(ct))
        {
            if (chunk.Type == StreamChunkType.Token)
            {
                Console.Write(chunk.Payload);
            }
        }

        Console.WriteLine();
        if (await completion is not GraphExecutionResult<TicketState>.Interrupted interrupted)
        {
            throw new InvalidOperationException("expected the graph to pause at the interrupt boundary");
        }

        logger.LogInformation(
            "Paused after '{SuspendedNode}', awaiting human approval before '{NextNode}'.",
            interrupted.SuspendedNode,
            interrupted.NextNode);

        // ---- A human approves the escalation; resume (Evict/Resume) ----
        var signal = new HumanSignal(
            IsApproved: true,
            ReviewerToken: "demo-reviewer",
            FieldOverrides: new Dictionary<string, string> { ["ResolvedBy"] = "demo-reviewer" });
        var resumeResult = await runner.ResumeAsync(ticketId, signal, cancellationToken: ct);
        if (resumeResult is not GraphExecutionResult<TicketState>.Completed completed)
        {
            throw new InvalidOperationException("expected the graph to complete after resume");
        }

        logger.LogInformation("Resolved: {Resolution}", completed.State.Resolution);

        // ---- Show what ended up in history ----
        var history = new List<ChatMessage>();
        await foreach (var message in historyStore.GetHistoryAsync(ticketId, ct: ct))
        {
            history.Add(message);
        }

        logger.LogInformation("History for {TicketId}:", ticketId);
        foreach (var message in history)
        {
            logger.LogInformation("  [{Role}] {Content}", message.Role, message.Content);
        }

        return (completed.State, history);
    }
}
