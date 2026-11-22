// Samples for https://stratamesh.dev/guides/tools/tool-middleware/
//
// A sample that starts with using lines has a namespace block of its own: using directives can only open a
// namespace, so each block's are the ones its sample shows.

// The page shows IToolMiddleware as Datalium.Strata.Tools declares it. This is a copy of that declaration, in a
// namespace of its own; TheDeclarationOnThePageIsTheEngines checks it against the engine's, so a change to the
// interface fails here.
namespace Datalium.Strata.DocsSnippets.Guides.Tools.ToolMiddleware.Declaration
{
    using Datalium.Strata.Tools;

    // docs:begin guides/tools/tool-middleware/declaration
    public delegate Task<string> ToolCallDelegate(ToolCall call, CancellationToken cancellationToken);

    public interface IToolMiddleware
    {
        Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken);

        Task OnRefusedAsync(ToolCall call, ToolNotFoundException refusal, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }
    // docs:end guides/tools/tool-middleware/declaration
}

namespace Datalium.Strata.DocsSnippets.Guides.Tools.ToolMiddleware
{
    // docs:begin guides/tools/tool-middleware/audit
    using System.Diagnostics;
    using Datalium.Strata.Tools;

    public sealed class AuditMiddleware : IToolMiddleware
    {
        public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            var clock = Stopwatch.StartNew();
            try
            {
                var result = await next(call, cancellationToken);
                Write(call, "succeeded", clock.Elapsed);
                return result;
            }
            catch (ToolCallDeniedException denied)
            {
                Write(call, $"denied ({denied.Reason})", clock.Elapsed);
                throw;
            }
            catch (Exception ex)
            {
                Write(call, $"failed ({ex.GetType().Name})", clock.Elapsed);
                throw;
            }
        }

        // Calls the registry refused before the chain ran: not found, or left out of a subset.
        public Task OnRefusedAsync(ToolCall call, ToolNotFoundException refusal, CancellationToken cancellationToken)
        {
            Write(call, refusal is ToolNotPermittedException ? "not permitted" : "not found", TimeSpan.Zero);
            return Task.CompletedTask;
        }

        // call.Arguments holds the JSON arguments. Log them only if they can't hold secrets.
        private static void Write(ToolCall call, string outcome, TimeSpan elapsed) =>
            Console.WriteLine(
                $"[audit] {call.Context.CallerId} called {call.Name}: {outcome} in {elapsed.TotalMilliseconds:0} ms");
    }
    // docs:end guides/tools/tool-middleware/audit

    // docs:begin guides/tools/tool-middleware/caller-policy
    public sealed class CallerPolicy(IReadOnlyDictionary<string, string[]> allowedCallers) : IToolMiddleware
    {
        public Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            if (allowedCallers.TryGetValue(call.Name, out var callers) && !callers.Contains(call.Context.CallerId))
            {
                throw new ToolCallDeniedException(call.Name, $"caller '{call.Context.CallerId}' may not call {call.Name}");
            }

            return next(call, cancellationToken);
        }
    }
    // docs:end guides/tools/tool-middleware/caller-policy

    // docs:begin guides/tools/tool-middleware/timeout
    public sealed class TimeoutMiddleware(TimeSpan limit) : IToolMiddleware
    {
        public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(limit);
            try
            {
                // The tool gets the linked token. WaitAsync stops waiting even if the tool ignores it.
                return await next(call, timeout.Token).WaitAsync(timeout.Token);
            }
            catch (OperationCanceledException) when (timeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"{call.Name} took longer than {limit.TotalSeconds:0} s");
            }
        }
    }
    // docs:end guides/tools/tool-middleware/timeout

    // docs:begin guides/tools/tool-middleware/concurrency-limit
    public sealed class ConcurrencyLimit(int maxCalls) : IToolMiddleware
    {
        private readonly SemaphoreSlim _slots = new(maxCalls, maxCalls);

        public async Task<string> InvokeAsync(ToolCall call, ToolCallDelegate next, CancellationToken cancellationToken)
        {
            await _slots.WaitAsync(cancellationToken);
            try
            {
                return await next(call, cancellationToken);
            }
            finally
            {
                _slots.Release();
            }
        }
    }
    // docs:end guides/tools/tool-middleware/concurrency-limit

    /// <summary>The tools of Define tools, with the slow inventory.recount this page adds.</summary>
    public partial class InventoryTools
    {
        private static readonly Dictionary<string, int> Stock = new() { ["AB-12"] = 7, ["CD-34"] = 0 };

        [StrataTool(Name = "inventory.lookup", Description = "Units in stock for a SKU.")]
        public static int Lookup(string sku, string? warehouse = null) =>
            Stock.GetValueOrDefault(sku);

        // docs:begin guides/tools/tool-middleware/recount
        // In InventoryTools, next to inventory.lookup.
        [StrataTool(Name = "inventory.recount", Description = "Counts the units of a SKU on the shelf.")]
        public static async Task<int> RecountAsync(string sku, CancellationToken cancellationToken)
        {
            // Stands in for a slow call to the warehouse system.
            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            return Stock.GetValueOrDefault(sku);
        }
        // docs:end guides/tools/tool-middleware/recount
    }

    public partial class BillingTools
    {
        [StrataTool(Name = "billing.refund", Description = "Refunds part or all of an order.")]
        public static string Refund(string orderId, decimal amount) =>
            FormattableString.Invariant($"refunded {amount:0.00} on {orderId}");
    }

    /// <summary>A tool that counts how many calls are inside it at once.</summary>
    public partial class BusyTools
    {
        private int _inside;
        private int _most;

        public int Most => Volatile.Read(ref _most);

        [StrataTool(Name = "probe.busy", Description = "Stays busy for a moment.")]
        public async Task BusyAsync(CancellationToken cancellationToken)
        {
            var now = Interlocked.Increment(ref _inside);
            int most;
            while (now > (most = Volatile.Read(ref _most)) && Interlocked.CompareExchange(ref _most, now, most) != most)
            {
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
            Interlocked.Decrement(ref _inside);
        }
    }

    /// <summary>The run test reads what the samples print, so nothing else may write to the console meanwhile.</summary>
    [CollectionDefinition(Name, DisableParallelization = true)]
    public sealed class ConsoleCollection
    {
        public const string Name = "guides/tools/tool-middleware: the console";
    }
}

namespace Datalium.Strata.DocsSnippets.Guides.Tools.ToolMiddleware
{
    using System.Reflection;
    using System.Text.Json;
    using System.Text.RegularExpressions;
    using Datalium.Strata.Tools;

    [Collection(ConsoleCollection.Name)]
    public class ToolMiddlewareSamples
    {
        [Fact]
        public async Task RegisterAndRunSomeCalls()
        {
            // docs:begin guides/tools/tool-middleware/register
            var tools = new ToolRegistry()
                .Register(new InventoryTools())
                .Register(new BillingTools())
                .Use(new AuditMiddleware())                       // outermost: sees every call first
                .Use(new CallerPolicy(new Dictionary<string, string[]>
                {
                    ["billing.refund"] = ["billing-agent"],
                }))
                .Use(new TimeoutMiddleware(TimeSpan.FromSeconds(1)))
                .Use(new ConcurrencyLimit(maxCalls: 4));          // innermost: next to the tool
            // docs:end guides/tools/tool-middleware/register

            Assert.Equal(
                [typeof(AuditMiddleware), typeof(CallerPolicy), typeof(TimeoutMiddleware), typeof(ConcurrencyLimit)],
                tools.Middleware.Select(m => m.GetType()));

            var console = Console.Out;
            using var printed = new StringWriter();
            Console.SetOut(printed);
            try
            {
                // docs:begin guides/tools/tool-middleware/run
                var support = ToolCallContext.None with { CallerId = "support-agent" };
                var billing = ToolCallContext.None with { CallerId = "billing-agent" };

                Console.WriteLine(await AskAsync(tools, "inventory.lookup", """{"sku":"AB-12"}""", support));
                Console.WriteLine(await AskAsync(tools, "billing.refund", """{"orderId":"A-1001","amount":40}""", support));
                Console.WriteLine(await AskAsync(tools, "billing.refund", """{"orderId":"A-1001","amount":40}""", billing));
                Console.WriteLine(await AskAsync(tools, "inventory.recount", """{"sku":"AB-12"}""", support));
                Console.WriteLine(await AskAsync(tools, "inventory.restock", """{"sku":"AB-12"}""", support));

                static async Task<string> AskAsync(ToolRegistry tools, string name, string arguments, ToolCallContext context)
                {
                    try
                    {
                        var json = await tools.DispatchToolAsync(name, arguments, context);
                        return JsonDocument.Parse(json).RootElement.GetProperty("result").ToString();
                    }
                    catch (ToolCallDeniedException denied)
                    {
                        return $"denied: {denied.Reason}";
                    }
                    catch (ToolNotFoundException refused)
                    {
                        return refused.Message;
                    }
                    catch (TimeoutException timedOut)
                    {
                        return $"failed: {timedOut.Message}";
                    }
                }
                // docs:end guides/tools/tool-middleware/run
            }
            finally
            {
                Console.SetOut(console);
            }

            // What the page shows, with every duration as "N ms": they differ from run to run.
            var lines = Regex.Replace(printed.ToString(), @"in \d+ ms", "in N ms").ReplaceLineEndings("\n").TrimEnd().Split('\n');
            Assert.Equal(
                [
                    "[audit] support-agent called inventory.lookup: succeeded in N ms",
                    "7",
                    "[audit] support-agent called billing.refund: denied (caller 'support-agent' may not call billing.refund) in N ms",
                    "denied: caller 'support-agent' may not call billing.refund",
                    "[audit] billing-agent called billing.refund: succeeded in N ms",
                    "refunded 40.00 on A-1001",
                    "[audit] support-agent called inventory.recount: failed (TimeoutException) in N ms",
                    "failed: inventory.recount took longer than 1 s",
                    "[audit] support-agent called inventory.restock: not found in N ms",
                    "Tool 'inventory.restock' was not found.",
                ],
                lines);
        }

        [Fact]
        public async Task TheConcurrencyLimitLetsTwoCallsThroughAtOnce()
        {
            var busy = new BusyTools();
            var tools = new ToolRegistry().Register(busy).Use(new ConcurrencyLimit(maxCalls: 2));

            await Task.WhenAll(Enumerable.Range(0, 6).Select(_ => tools.DispatchToolAsync("probe.busy", "{}")));

            Assert.Equal(2, busy.Most);
        }

        [Fact]
        public async Task AMiddlewareCanBeTestedAlone()
        {
            // As "Test a middleware" says: a ToolCall of your own, and a delegate for next.
            var policy = new CallerPolicy(new Dictionary<string, string[]> { ["billing.refund"] = ["billing-agent"] });
            var call = new ToolCall("billing.refund", "{}", ToolCallContext.None with { CallerId = "x" });

            var denied = await Assert.ThrowsAsync<ToolCallDeniedException>(
                () => policy.InvokeAsync(call, (_, _) => Task.FromResult("{}"), CancellationToken.None));
            Assert.Equal("caller 'x' may not call billing.refund", denied.Reason);
        }

        [Fact]
        public void TheDeclarationOnThePageIsTheEngines()
        {
            static string Signature(MethodInfo method) =>
                $"{method.ReturnType.Name} {method.Name}("
                + string.Join(", ", method.GetParameters().Select(p => $"{p.ParameterType.Name} {p.Name}"))
                + $") {(method.IsAbstract ? "abstract" : "with a default")}";

            Assert.Equal(
                typeof(IToolMiddleware).GetMethods().Select(Signature).Order(),
                typeof(Declaration.IToolMiddleware).GetMethods().Select(Signature).Order());
            Assert.Equal(
                Signature(typeof(ToolCallDelegate).GetMethod("Invoke")!),
                Signature(typeof(Declaration.ToolCallDelegate).GetMethod("Invoke")!));
        }
    }
}
