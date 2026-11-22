// Samples for https://stratamesh.dev/guides/ai/build-an-agent/

using System.Runtime.CompilerServices;
using System.Text.Json;
using Datalium.Strata.AI;
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Tools;
using Microsoft.Extensions.AI;

namespace Datalium.Strata.DocsSnippets.Guides.AI.BuildAnAgent;

// docs:begin guides/ai/build-an-agent/answer-type
sealed record RefundDecision(string OrderId, string Outcome, decimal Refunded, string? RefundId, string Reply);

// docs:hide
[Collection(BuildAnAgentConsole.Name)]
public class BuildAnAgentSamples
{
    // docs:show
    static IReadOnlyList<string> Validate(RefundDecision decision)
    {
        var problems = new List<string>();
        if (decision.Outcome is not ("refunded" or "declined" or "escalated"))
        {
            problems.Add($"outcome '{decision.Outcome}' is not one of refunded, declined, escalated");
        }

        if (decision.Outcome == "refunded" && (decision.RefundId is null || decision.Refunded <= 0))
        {
            problems.Add("a refund needs a refund id and an amount");
        }

        return problems;
    }
    // docs:end guides/ai/build-an-agent/answer-type

    [Fact]
    public async Task RunTheSupportAgent()
    {
        using var console = new ConsoleOutput();

        // docs:begin guides/ai/build-an-agent/subset
        var desk = new SupportDesk();
        var tools = new ToolRegistry()
            .Register(desk)
            .Subset("orders.lookup", "shipping.track", "refunds.issue");
        // docs:end guides/ai/build-an-agent/subset

        // docs:begin guides/ai/build-an-agent/script
        IChatClient chatClient = new ScriptedModel(
            _ => Reply.Call(("call_1", "orders_lookup", """{"orderId":"A-1001"}""")),
            _ => Reply.Call(("call_2", "refunds_issue", """{"orderId":"A-1001","amount":39.90,"reason":"damaged in transit"}""")),
            _ => Reply.Say("""{"orderId":"A-1001","outcome":"refunded","refunded":39.90,"refundId":"RF-0001","reply":"Sorry about the box. I have refunded 39.90 (RF-0001)."}"""));
        // docs:end guides/ai/build-an-agent/script

        // docs:begin guides/ai/build-an-agent/create-agent
        var agent = new StrataAgent<RefundDecision>(chatClient, tools, new StrataAgentOptions<RefundDecision>
        {
            Id = "support-agent",
            Chat = new ChatOptions { ModelId = "gpt-5-mini", Temperature = 0 },
            MaxTurns = 6,
            Validate = Validate,
        });
        // docs:end guides/ai/build-an-agent/create-agent

        // The subset's tools, under provider-safe names, mapped back to the Strata names. accounts.close is left out.
        Assert.Equal(["orders_lookup", "shipping_track", "refunds_issue"], agent.Declarations.Tools.Select(t => t.Name));
        Assert.Equal("orders.lookup", agent.Declarations.StrataName("orders_lookup"));
        Assert.Null(agent.Declarations.ProviderNameOf("accounts.close"));

        var graph = RefundGraph(agent);

        // docs:begin guides/ai/build-an-agent/run
        var store = new InMemoryCheckpointStore();
        var result = await graph.RunAsync(
            "ticket-1",
            new Ticket("T-1", "C-77", "My order A-1001 arrived with the box crushed."),
            store);

        if (result is GraphExecutionResult<Ticket>.Completed { State: var done })
        {
            Console.WriteLine($"{done.Decision!.Outcome} after {done.Turns} turns");
            Console.WriteLine(done.Sent);
        }
        else
        {
            Console.WriteLine($"The run did not complete: {result}");
        }
        // docs:end guides/ai/build-an-agent/run

        // What the page says the run prints with this script.
        Assert.Equal(
            "refunded after 3 turns\nto C-77: Sorry about the box. I have refunded 39.90 (RF-0001).\n",
            console.Text);

        // Turn 1 looked the order up, turn 2 refunded it, turn 3 answered. Every turn asked for the agent's model.
        var model = (ScriptedModel)chatClient;
        Assert.Equal(3, model.Requests.Count);
        Assert.All(model.Requests, r => Assert.Equal("gpt-5-mini", r.Options!.ModelId));
        Assert.Equal(["orders.lookup(A-1001)", "refunds.issue(A-1001, 39.90, damaged in transit)"], desk.Calls);
        Assert.Equal([("RF-0001", "A-1001", 39.90m)], desk.Refunds);
    }

    [Fact]
    public async Task ARunawayLoopFailsTransiently()
    {
        // The agent of step 3, MaxTurns 6, with a model that never stops calling tools.
        var desk = new SupportDesk();
        var tools = new ToolRegistry().Register(desk).Subset("orders.lookup", "shipping.track", "refunds.issue");
        var polls = Enumerable.Range(1, 7)
            .Select(i => (Func<IReadOnlyList<ChatMessage>, Reply>)(_ => Reply.Call(($"poll_{i}", "shipping_track", """{"orderId":"A-1002"}"""))))
            .ToArray();
        var agent = new StrataAgent<RefundDecision>(new ScriptedModel(polls), tools, new StrataAgentOptions<RefundDecision>
        {
            Id = "support-agent",
            MaxTurns = 6,
            Validate = Validate,
        });
        var graph = new StateGraph<Ticket>()
            .AddAgentNode("Resolve", agent, (ticket, _) => [new(ChatRole.User, ticket.Message)], (ticket, r) => ticket with { Decision = r.Output })
            .Compile();

        var result = await graph.RunAsync("ticket-runaway", new Ticket("T-9", "C-77", "Where is A-1002?"), new InMemoryCheckpointStore());

        // One tool turn more than MaxTurns fails the node, before that turn's tools run.
        var failed = Assert.IsType<GraphExecutionResult<Ticket>.NodeFailed>(result);
        Assert.Equal("agent 'support-agent' made 6 tool turns without answering", failed.Message);
        Assert.Equal(6, desk.Calls.Count);
    }

    private static CompiledGraph<Ticket> RefundGraph(StrataAgent<RefundDecision> agent)
    {
        // docs:begin guides/ai/build-an-agent/graph
        var graph = new StateGraph<Ticket>()
            .AddAgentNode("Resolve", agent,
                prompt: (ticket, ctx) =>
                [
                    new(ChatRole.System,
                        "You are a support agent. Look an order up before deciding anything. Refund an order that " +
                        "arrived damaged within 30 days of delivery; decline other refunds; escalate what you can't do."),
                    new(ChatRole.User, $"Ticket {ticket.Id} from customer {ticket.Customer}: {ticket.Message}"),
                ],
                apply: (ticket, result) => ticket with { Decision = result.Output, Turns = result.Turns })
            .AddNode("Notify", ticket => ticket with { Sent = $"to {ticket.Customer}: {ticket.Decision!.Reply}" })
            .AddEdge("Resolve", "Notify")
            .SetRecoveryPolicy("Resolve", RecoveryPolicy.Retry(1))
            .Compile();

        // docs:hide
        return graph;
    }

    // docs:show
    sealed record Ticket(
        string Id, string Customer, string Message,
        RefundDecision? Decision = null, int Turns = 0, string? Sent = null);
    // docs:end guides/ai/build-an-agent/graph
}

public class BuildAnAgentApprovalSamples
{
    [Fact]
    public async Task PutAPersonBeforeTheRefund()
    {
        var desk = new SupportDesk();
        var store = new InMemoryCheckpointStore();
        IChatClient chatClient = new ScriptedModel(
            _ => Reply.Call(("call_1", "orders_lookup", """{"orderId":"A-1001"}""")),
            _ => Reply.Say("""{"orderId":"A-1001","amount":39.90,"reason":"damaged in transit"}"""));

        // docs:begin guides/ai/build-an-agent/approval
        var deskTools = new ToolRegistry().Register(desk);

        // The agent may only read. It can't call refunds.issue.
        var proposer = new StrataAgent<RefundProposal>(
            chatClient, deskTools.Subset("orders.lookup", "shipping.track"),
            new StrataAgentOptions<RefundProposal> { Id = "refund-proposer" });

        var approvalGraph = new StateGraph<Ticket>()
            .AddAgentNode("Propose", proposer, Prompt, (ticket, result) => ticket with { Proposal = result.Output })
            .AddInterruptBoundary("Propose")
            .AddFallibleNode("IssueRefund", async (ticket, ctx, ct) =>
            {
                var arguments = JsonSerializer.Serialize(ticket.Proposal, JsonSerializerOptions.Web);
                var refund = await deskTools.DispatchToolAsync("refunds.issue", arguments, ToolCallContext.From(ctx), ct);
                return ticket with { Refund = refund };
            })
            .AddEdge("Propose", "IssueRefund")
            .Compile();

        // Runs Propose, saves the thread, and returns Interrupted. Nothing is refunded yet.
        var paused = await approvalGraph.RunAsync("ticket-2", new Ticket("T-2", "C-77", "A-1001 arrived broken."), store);

        // Later: a person reads the proposal in the saved state and approves it.
        var resumed = await approvalGraph.ResumeAsync("ticket-2", HumanSignal.Approve("lead@example.com"), store);
        // docs:end guides/ai/build-an-agent/approval

        // The run paused after Propose with the proposal in the state and no refund.
        var interrupted = Assert.IsType<GraphExecutionResult<Ticket>.Interrupted>(paused);
        Assert.Equal(new RefundProposal("A-1001", 39.90m, "damaged in transit"), interrupted.FrozenState.Proposal);
        Assert.Null(interrupted.FrozenState.Refund);

        // The approval ran IssueRefund: one refund, through the full registry.
        var completed = Assert.IsType<GraphExecutionResult<Ticket>.Completed>(resumed);
        Assert.Contains("RF-0001", completed.State.Refund);
        Assert.Equal([("RF-0001", "A-1001", 39.90m)], desk.Refunds);
        Assert.Equal(["orders.lookup(A-1001)", "refunds.issue(A-1001, 39.90, damaged in transit)"], desk.Calls);
    }

    static IEnumerable<ChatMessage> Prompt(Ticket ticket, NodeContext context) =>
    [
        new(ChatRole.System, "You are a support agent. Look an order up, then propose a refund for a person to approve."),
        new(ChatRole.User, $"Ticket {ticket.Id} from customer {ticket.Customer}: {ticket.Message}"),
    ];

    sealed record RefundProposal(string OrderId, decimal Amount, string Reason);

    sealed record Ticket(
        string Id, string Customer, string Message, RefundProposal? Proposal = null, string? Refund = null);
}

/// <summary>An order as <c>orders.lookup</c> returns it.</summary>
public sealed record OrderInfo(string OrderId, string CustomerId, decimal Total, string Status, int DaysSinceDelivery);

// docs:begin guides/ai/build-an-agent/define-tool
public partial class SupportDesk
{
    // docs:hide ...
    private readonly Dictionary<string, OrderInfo> _orders = new(StringComparer.Ordinal)
    {
        ["A-1001"] = new("A-1001", "C-77", 39.90m, "delivered, box crushed in transit", 3),
        ["A-1002"] = new("A-1002", "C-77", 129.00m, "in transit", 0),
    };

    private int _nextRefund = 1;

    /// <summary>Every tool that ran, as "name(arguments)", in order.</summary>
    public List<string> Calls { get; } = [];

    /// <summary>Refunds issued: (refund id, order, amount).</summary>
    public List<(string RefundId, string OrderId, decimal Amount)> Refunds { get; } = [];

    // docs:show
    [StrataTool(Name = "orders.lookup", Description = "Looks an order up by its id: customer, total, status, days since delivery.")]
    public OrderInfo? Lookup(string orderId)
    {
        // docs:hide ...
        Calls.Add($"orders.lookup({orderId})");
        return _orders.GetValueOrDefault(orderId);
        // docs:show
    }
    // docs:hide ...

    [StrataTool(Name = "shipping.track", Description = "Where an order's parcel is now.")]
    public string Track(string orderId)
    {
        Calls.Add($"shipping.track({orderId})");
        return _orders.TryGetValue(orderId, out var order) && order.Status == "in transit"
            ? "in transit: left the Rotterdam hub, no delivery date yet"
            : "delivered";
    }

    [StrataTool(Name = "refunds.issue", Description = "Refunds an amount of an order to the customer's card. Returns the refund id.")]
    public string Refund(string orderId, decimal amount, string reason)
    {
        Calls.Add($"refunds.issue({orderId}, {amount}, {reason})");
        var id = $"RF-{_nextRefund++:D4}";
        Refunds.Add((id, orderId, amount));
        return id;
    }

    [StrataTool(Name = "accounts.close", Description = "Closes a customer's account. Not for the support agent.")]
    public string CloseAccount(string customerId)
    {
        Calls.Add($"accounts.close({customerId})");
        return "closed";
    }
    // docs:show
}
// docs:end guides/ai/build-an-agent/define-tool

/// <summary>What the stand-in model says in one turn: text, tool calls, or both.</summary>
public sealed record Reply(string Text, IReadOnlyList<(string Id, string Name, string Arguments)> Calls)
{
    public static Reply Say(string text) => new(text, []);

    public static Reply Call(params (string Id, string Name, string Arguments)[] calls) => new(string.Empty, calls);
}

/// <summary>One request the agent sent the model, and the usage the model reported for it.</summary>
public sealed record Request(IReadOnlyList<ChatMessage> Messages, ChatOptions? Options, long InputTokens, long OutputTokens);

// docs:begin guides/ai/build-an-agent/scripted-model
public sealed class ScriptedModel(params Func<IReadOnlyList<ChatMessage>, Reply>[] turns) : IChatClient
{
    private readonly Queue<Func<IReadOnlyList<ChatMessage>, Reply>> _turns = new(turns);

    public List<Request> Requests { get; } = [];

    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        var conversation = messages.ToList();
        var turn = _turns.Count > 0 ? _turns.Dequeue() : throw new InvalidOperationException("the script has no more turns");
        var reply = turn(conversation);
        // docs:hide ...

        // Made-up but deterministic token counts: 40 per message read, 10 per answer plus 15 per tool call.
        var (input, output) = (40L * conversation.Count, 10L + (15L * reply.Calls.Count) + (reply.Text.Length / 4));
        Requests.Add(new Request(conversation, options, input, output));

        // docs:show
        List<AIContent> contents = reply.Text.Length == 0 ? [] : [new TextContent(reply.Text)];
        contents.AddRange(reply.Calls.Select(c => FunctionCallContent.CreateFromParsedArguments(
            c.Arguments, c.Id, c.Name, static json => JsonSerializer.Deserialize<Dictionary<string, object?>>(json)!)));
        return Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, contents))
        {
            // docs:hide ...
            ModelId = options?.ModelId ?? "support-model-stand-in",
            FinishReason = reply.Calls.Count > 0 ? ChatFinishReason.ToolCalls : ChatFinishReason.Stop,
            Usage = new UsageDetails { InputTokenCount = input, OutputTokenCount = output },
            // docs:show
        });
    }
    // docs:hide ...

    public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages, ChatOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var response = await GetResponseAsync(messages, options, cancellationToken);
        foreach (var update in response.ToChatResponseUpdates())
        {
            yield return update;
        }
    }

    public object? GetService(Type serviceType, object? serviceKey = null) =>
        serviceType == typeof(ChatClientMetadata) ? new ChatClientMetadata("scripted", null, "support-model-stand-in") : null;

    public void Dispose()
    {
    }
    // docs:show
}
// docs:end guides/ai/build-an-agent/scripted-model

/// <summary>The tests that check what a sample prints run alone, so nothing else writes to the console meanwhile.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class BuildAnAgentConsole
{
    public const string Name = "guides/ai/build-an-agent console";
}

/// <summary>Captures what a sample writes to the console, with \n line endings.</summary>
internal sealed class ConsoleOutput : IDisposable
{
    private readonly TextWriter _original = Console.Out;
    private readonly StringWriter _writer = new();

    public ConsoleOutput() => Console.SetOut(_writer);

    public string Text => _writer.ToString().ReplaceLineEndings("\n");

    public void Dispose() => Console.SetOut(_original);
}
