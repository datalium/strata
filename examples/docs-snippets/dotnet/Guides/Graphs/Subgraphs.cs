// Samples for https://stratamesh.dev/guides/graphs/subgraphs/

using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;
using Datalium.Strata.Storage.Schema;

namespace Datalium.Strata.DocsSnippets.Guides.Graphs.Subgraphs;

public class SubgraphsSamples
{
    // LookupInvoice -> (IssueRefund if it was a duplicate) -> DraftReply. With needsApproval, a human signs the refund
    // off first, at the interrupt boundary AwaitApproval.
    private static CompiledGraph<BillingCase> BuildBillingAgent(BillingApi api, int ownRetries, bool needsApproval = false)
    {
        var graph = new StateGraph<BillingCase>()
            .AddFallibleNode("LookupInvoice", billing =>
            {
                var (chargeCents, duplicate) = api.LatestInvoice();
                return billing with
                {
                    ChargeCents = chargeCents,
                    Duplicate = duplicate,
                    Steps = [.. billing.Steps, $"found invoice for {Dollars(chargeCents)}{(duplicate ? " (charged twice)" : "")}"],
                };
            })
            .SetRecoveryPolicy("LookupInvoice", RecoveryPolicy.Retry(ownRetries))
            .AddNode("IssueRefund", billing =>
            {
                var credit = billing.CreditCents > 0 ? $" and credited {Dollars(billing.CreditCents)}" : "";
                return billing with { Steps = [.. billing.Steps, $"refunded {Dollars(billing.ChargeCents)}{credit}"] };
            })
            .AddNode("DraftReply", billing =>
            {
                var reply = "Your invoice is correct as issued.";
                if (billing.Duplicate)
                {
                    reply = $"We charged you twice and have refunded {Dollars(billing.ChargeCents)}.";
                    if (billing.CreditCents > 0)
                    {
                        reply += $" We have also added a {Dollars(billing.CreditCents)} credit.";
                    }
                }

                return billing with { Reply = reply, Steps = [.. billing.Steps, "drafted reply"] };
            });

        var refundStep = "IssueRefund";
        if (needsApproval)
        {
            refundStep = "AwaitApproval";
            graph
                .AddNode("AwaitApproval", billing => billing with
                {
                    Steps = [.. billing.Steps, $"refund of {Dollars(billing.ChargeCents)} awaits approval"],
                })
                .AddInterruptBoundary("AwaitApproval")
                .AddEdge("AwaitApproval", "IssueRefund");
        }

        return graph
            .AddConditionalEdge("LookupInvoice", refundStep, billing => billing.Duplicate)
            .AddEdge("LookupInvoice", "DraftReply")
            .AddEdge("IssueRefund", "DraftReply")
            .Compile();
    }

    // docs:begin guides/graphs/subgraphs/child
    static CompiledGraph<Probe> BuildDiagnostics(Device device, Counter? networkChecks) =>
        new StateGraph<Probe>()
            .AddNode("CheckNetwork", probe =>
            {
                networkChecks?.Increment();
                return probe with { Findings = [.. probe.Findings, "network ok"] };
            })
            .AddFallibleNode("CheckFirmware", probe => device switch
            {
                Device.Healthy => probe with { Findings = [.. probe.Findings, "firmware 2.1 ok"] },
                _ => throw NodeException.Fatal("firmware 1.0 is too old to diagnose remotely"),
            })
            .AddNode("Summarize", probe => probe with
            {
                Findings = [.. probe.Findings, $"{probe.Findings.Count} checks passed"],
            })
            .AddEdge("CheckNetwork", "CheckFirmware")
            .AddEdge("CheckFirmware", "Summarize")
            .Compile();

    sealed record Probe(IReadOnlyList<string> Findings);
    // docs:end guides/graphs/subgraphs/child

    // docs:begin guides/graphs/subgraphs/add-subgraph
    static CompiledGraph<TechCase> BuildTechAgent(Device device, Counter? networkChecks = null) =>
        new StateGraph<TechCase>()
            .AddSubgraph(
                "Diagnose",
                BuildDiagnostics(device, networkChecks),
                _ => new Probe([]),
                (tech, probe) => tech with
                {
                    Steps = [.. tech.Steps, .. probe.Findings.Select(finding => $"diagnostics: {finding}")],
                })
            // docs:hide ... ApplyFix and DraftReply
            .AddNode("ApplyFix", tech =>
            {
                var attempts = tech.Attempts + 1;
                // The stand-in device only responds to the second attempt.
                var fixedNow = attempts >= 2;
                return tech with
                {
                    Attempts = attempts,
                    Fixed = fixedNow,
                    Steps = [.. tech.Steps, $"fix attempt {attempts}: {(fixedNow ? "resolved" : "no change")}"],
                };
            })
            .AddNode("DraftReply", tech => tech with
            {
                Reply = $"Fixed after {tech.Attempts} attempt(s).",
                Steps = [.. tech.Steps, "drafted reply"],
            })
            // docs:show
            .AddConditionalEdge("ApplyFix", "ApplyFix", tech => !tech.Fixed && tech.Attempts < 3)
            .AddEdge("ApplyFix", "DraftReply")
            .AddEdge("Diagnose", "ApplyFix")
            .Compile();
    // docs:end guides/graphs/subgraphs/add-subgraph

    // Triage -> BillingAgent | TechAgent | GeneralReply -> SendReply. When the policy falls back, Escalate is its
    // fallback node.
    private static CompiledGraph<Ticket> BuildSupportDesk(
        CompiledGraph<BillingCase> billing, CompiledGraph<TechCase> tech, DeskPolicy policy)
    {
        // docs:begin guides/graphs/subgraphs/route
        var graph = new StateGraph<Ticket>()
            // docs:hide ... Triage sets Category to "billing", "tech" or "general"
            .AddNode("Triage", ticket =>
            {
                // Keyword matching stands in for an LLM call.
                var text = ticket.Message.ToLowerInvariant();
                var category = new[] { "charge", "invoice", "refund" }.Any(text.Contains) ? "billing"
                    : new[] { "router", "wifi", "crash" }.Any(text.Contains) ? "tech"
                    : "general";
                return ticket with { Category = category, Log = [.. ticket.Log, $"triage: {category}"] };
            })
            // docs:show
            .AddSubgraph(
                "BillingAgent",
                billing,
                _ => BillingCase.New(),
                (ticket, result) => ticket with
                {
                    Reply = result.Reply,
                    Log = [.. ticket.Log, .. result.Steps.Select(step => $"billing: {step}")],
                })
            .AddSubgraph(
                "TechAgent",
                tech,
                _ => TechCase.New(),
                (ticket, result) => ticket with
                {
                    Reply = result.Reply,
                    Log = [.. ticket.Log, .. result.Steps.Select(step => $"tech: {step}")],
                })
            // docs:hide ... GeneralReply and SendReply
            .AddNode("GeneralReply", ticket => ticket with { Reply = "Thanks for getting in touch." })
            .AddNode("SendReply", ticket => ticket with { Log = [.. ticket.Log, "reply sent"] })
            // docs:show
            .AddConditionalEdge("Triage", "BillingAgent", ticket => ticket.Category == "billing")
            .AddConditionalEdge("Triage", "TechAgent", ticket => ticket.Category == "tech")
            .AddEdge("Triage", "GeneralReply")
            .AddEdge("BillingAgent", "SendReply")
            .AddEdge("TechAgent", "SendReply")
            .AddEdge("GeneralReply", "SendReply");
        // docs:end guides/graphs/subgraphs/route

        // docs:begin guides/graphs/subgraphs/recovery
        var recovery = RecoveryPolicy.Retry(policy.Retries);
        if (policy.Fallback)
        {
            graph
                .AddNode("Escalate", ticket => ticket with
                {
                    // docs:hide ...
                    Reply = "A human agent will contact you shortly.",
                    Log = [.. ticket.Log, "escalated to a human"],
                    // docs:show
                })
                .AddEdge("Escalate", "SendReply");
            recovery = recovery.WithFallback("Escalate");
        }

        return graph
            .SetRecoveryPolicy("BillingAgent", recovery)
            .SetRecoveryPolicy("TechAgent", recovery)
            .Compile();
        // docs:end guides/graphs/subgraphs/recovery
    }

    private static CompiledGraph<Ticket> BuildDesk(
        BillingApi? api = null, Device device = Device.Healthy, DeskPolicy? policy = null, int ownRetries = 0,
        bool needsApproval = false, Counter? networkChecks = null) =>
        BuildSupportDesk(
            BuildBillingAgent(api ?? new BillingApi(failFirst: 0), ownRetries, needsApproval),
            BuildTechAgent(device, networkChecks),
            policy ?? new DeskPolicy(Retries: 0, Fallback: false));

    private static readonly string[] FixedTechLog =
    [
        "triage: tech",
        "tech: diagnostics: network ok",
        "tech: diagnostics: firmware 2.1 ok",
        "tech: diagnostics: 2 checks passed",
        "tech: fix attempt 1: no change",
        "tech: fix attempt 2: resolved",
        "tech: drafted reply",
        "reply sent",
    ];

    [Fact]
    public async Task ATechTicketGoesThreeLevelsDeep()
    {
        var result = await BuildDesk().RunAsync("subgraphs-2", Ticket.New("My router keeps crashing"), new InMemoryCheckpointStore());

        var ticket = Assert.IsType<GraphExecutionResult<Ticket>.Completed>(result).State;
        Assert.Equal("Fixed after 2 attempt(s).", ticket.Reply);
        Assert.Equal(FixedTechLog, ticket.Log);
    }

    [Fact]
    public async Task AChildUsesTheParentsStateType()
    {
        var result = await BuildJobGraph().RunAsync("subgraphs-job", new Job(5, []), new InMemoryCheckpointStore());

        var job = Assert.IsType<GraphExecutionResult<Job>.Completed>(result).State;
        Assert.Equal(10, job.N);
        Assert.Equal(["prepare", "double"], job.Log);
    }

    private static CompiledGraph<Job> BuildJobGraph()
    {
        // docs:begin guides/graphs/subgraphs/same-type
        var work = new StateGraph<Job>()
            .AddNode("Double", job => job with { N = job.N * 2, Log = [.. job.Log, "double"] })
            .Compile();

        var graph = new StateGraph<Job>()
            .AddNode("Prepare", job => job with { Log = [.. job.Log, "prepare"] })
            .AddSubgraph("Work", work, job => job, (_, result) => result)
            .AddEdge("Prepare", "Work")
            .Compile();

        // docs:hide
        return graph;
    }

    // docs:show
    sealed record Job(int N, IReadOnlyList<string> Log);
    // docs:end guides/graphs/subgraphs/same-type

    [Fact]
    public async Task AChildRecoversItself()
    {
        // The billing API times out twice, and the child's own Retry(2) handles it.
        var api = new BillingApi(failFirst: 2);
        var store = new InMemoryCheckpointStore();
        var result = await BuildDesk(api, ownRetries: 2).RunAsync("subgraphs-3", Ticket.New("Please refund my invoice"), store);

        Assert.IsType<GraphExecutionResult<Ticket>.Completed>(result);
        Assert.Equal(3, api.Calls);
        Assert.Null(store.Load("subgraphs-3"));
    }

    [Fact]
    public async Task TheParentsPolicyTreatsTheChildAsOneNode()
    {
        // The billing API is down: the child retries once, the desk retries the whole child once, then falls back.
        var api = new BillingApi(failFirst: int.MaxValue);
        var desk = BuildDesk(api, policy: new DeskPolicy(Retries: 1, Fallback: true), ownRetries: 1);
        var result = await desk.RunAsync("subgraphs-4", Ticket.New("Where is my refund?"), new InMemoryCheckpointStore());

        var ticket = Assert.IsType<GraphExecutionResult<Ticket>.Completed>(result).State;
        Assert.Equal(4, api.Calls);
        Assert.Equal(["triage: billing", "escalated to a human", "reply sent"], ticket.Log);
    }

    [Fact]
    public async Task AFatalErrorTwoLevelsDown()
    {
        var store = new InMemoryCheckpointStore();
        var desk = BuildDesk(device: Device.OldFirmware, policy: new DeskPolicy(Retries: 3, Fallback: false));
        var result = await desk.RunAsync("subgraphs-5", Ticket.New("My wifi is down"), store);

        // The fatal kind survives both levels, so none of the desk's retries is used.
        var failed = Assert.IsType<GraphExecutionResult<Ticket>.NodeFailed>(result);
        Assert.Equal(("TechAgent", 1), (failed.Node, failed.Attempts));
        Assert.Equal(
            "subgraph 'TechAgent' failed at 'Diagnose' after 1 attempt(s): " +
            "subgraph 'Diagnose' failed at 'CheckFirmware' after 1 attempt(s): " +
            "firmware 1.0 is too old to diagnose remotely",
            failed.Message);

        var checkpoint = store.Load("subgraphs-5");
        Assert.NotNull(checkpoint);
        Assert.Equal(Datalium.Strata.Interruption.CheckpointStatus.Failed, checkpoint.Status);
        Assert.Equal("fatal", checkpoint.Metadata!["error_kind"]);
        Assert.Equal("[\"triage: tech\"]", checkpoint.FrozenState!["Log"]!.ToJsonString());
        Assert.Equal(["TechAgent", "Diagnose", "CheckFirmware"], checkpoint.WaitingAt());

        // Once the firmware is fixed, a resume re-enters Diagnose at CheckFirmware: CheckNetwork doesn't run again.
        var networkChecks = new Counter();
        var fixedDesk = BuildDesk(policy: new DeskPolicy(Retries: 3, Fallback: false), networkChecks: networkChecks);
        var resumed = await fixedDesk.ResumeAsync("subgraphs-5", HumanSignal.Approve("ops@support.example"), store);

        Assert.Equal(FixedTechLog, Assert.IsType<GraphExecutionResult<Ticket>.Completed>(resumed).State.Log);
        Assert.Equal(0, networkChecks.Count);
    }

    [Fact]
    public async Task PauseInsideASubgraph()
    {
        var desk = BuildDesk(needsApproval: true);
        var store = new InMemoryCheckpointStore();
        IReadOnlyList<string>? pausedAt = null;
        Ticket? finished = null;

        // docs:begin guides/graphs/subgraphs/pause
        var result = await desk.RunAsync("ticket-7", Ticket.New("I was charged twice for invoice 1042"), store);
        if (result is GraphExecutionResult<Ticket>.Interrupted paused)
        {
            Console.WriteLine($"Paused at {string.Join("/", paused.WaitingAt)}");
            // docs:hide
            pausedAt = paused.WaitingAt;
            // docs:show
        }

        // The first signal answers the innermost level: BillingAgent's boundary.
        // .At([], ...) adds overrides for the desk's own state.
        var signals = new LevelSignals(HumanSignal.Approve("dana@support.example").WithOverride("CreditCents", 500))
            .At([], HumanSignal.Approve("dana@support.example").WithOverride("Category", "billing-priority"));
        var resumed = await desk.ResumeAsync("ticket-7", signals, store);
        if (resumed is GraphExecutionResult<Ticket>.Completed { State: var ticket })
        {
            Console.WriteLine($"Category: {ticket.Category}");
            Console.WriteLine($"Reply: {ticket.Reply}");
            // docs:hide
            finished = ticket;
            // docs:show
        }
        // docs:end guides/graphs/subgraphs/pause

        Assert.Equal(["BillingAgent", "AwaitApproval"], pausedAt);
        Assert.NotNull(finished);
        Assert.Equal("billing-priority", finished.Category);
        Assert.Equal("We charged you twice and have refunded $49.99. We have also added a $5.00 credit.", finished.Reply);
    }

    [Fact]
    public void TheDefinitionNestsEachChild()
    {
        var definition = BuildDesk(needsApproval: true, policy: new DeskPolicy(Retries: 1, Fallback: true)).ToDefinition();

        var techAgent = definition.Nodes.Single(node => node.Name == "TechAgent");
        Assert.Equal(GraphNodeKind.Subgraph, techAgent.Kind);
        var diagnose = techAgent.Graph!.Nodes.Single(node => node.Name == "Diagnose");
        Assert.Equal(["CheckNetwork", "CheckFirmware", "Summarize"], diagnose.Graph!.Nodes.Select(node => node.Name));
    }

    private static string Dollars(long cents) => $"${cents / 100}.{cents % 100:00}";
}

/// <summary>The desk's state.</summary>
internal sealed record Ticket(string Message, string? Category, string? Reply, IReadOnlyList<string> Log)
{
    public static Ticket New(string message) => new(message, Category: null, Reply: null, Log: []);
}

/// <summary>BillingAgent's state. A reviewer's override sets <c>CreditCents</c>, a goodwill credit.</summary>
internal sealed record BillingCase(
    long ChargeCents, bool Duplicate, string Reply, IReadOnlyList<string> Steps, long CreditCents = 0)
{
    public static BillingCase New() => new(ChargeCents: 0, Duplicate: false, Reply: "", Steps: []);
}

/// <summary>TechAgent's state.</summary>
internal sealed record TechCase(int Attempts, bool Fixed, string Reply, IReadOnlyList<string> Steps)
{
    public static TechCase New() => new(Attempts: 0, Fixed: false, Reply: "", Steps: []);
}

internal enum Device
{
    Healthy,

    /// <summary>Its firmware is too old to diagnose remotely: retrying can't help.</summary>
    OldFirmware,
}

/// <summary>How the desk reacts when an agent fails.</summary>
internal sealed record DeskPolicy(int Retries, bool Fallback);

internal sealed class Counter
{
    private int _count;

    public int Count => Volatile.Read(ref _count);

    public void Increment() => Interlocked.Increment(ref _count);
}

/// <summary>The billing system. Times out for its first <c>failFirst</c> calls.</summary>
internal sealed class BillingApi(int failFirst)
{
    private int _calls;

    public int Calls => Volatile.Read(ref _calls);

    /// <summary>The customer's latest invoice: its amount in cents, and whether it was charged twice.</summary>
    public (long ChargeCents, bool Duplicate) LatestInvoice()
    {
        var call = Interlocked.Increment(ref _calls);
        if (call <= failFirst)
        {
            throw NodeException.Transient("billing API timed out");
        }

        return (4999, true);
    }
}
