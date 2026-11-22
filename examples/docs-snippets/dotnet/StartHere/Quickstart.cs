// Samples for https://stratamesh.dev/start-here/quickstart/

// docs:begin start-here/quickstart/program
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

// docs:hide
using Datalium.Strata.Validation;

namespace Datalium.Strata.DocsSnippets.StartHere.Quickstart;

// The sample is the whole of Program.cs, which prints its result: the test reads it from the console.
[Collection(QuickstartConsole.Name)]
public class QuickstartSamples
{
    [Fact]
    public async Task Program()
    {
        var output = new StringWriter();
        var console = Console.Out;
        Console.SetOut(output);
        try
        {
            // docs:show
            var graph = new StateGraph<Greeting>()
                .AddNode("Compose", state => state with { Message = $"Hello, {state.Name}" })
                .AddNode("Punctuate", state => state with { Message = state.Message + "!" })
                .AddEdge("Compose", "Punctuate")
                .Compile();

            var store = new InMemoryCheckpointStore();
            var result = await graph.RunAsync("greeting-1", new Greeting("Ada", ""), store);

            if (result is GraphExecutionResult<Greeting>.Completed completed)
            {
                Console.WriteLine(completed.State.Message);
            }
            else
            {
                Console.WriteLine("The run did not complete.");
            }

            // docs:hide
        }
        finally
        {
            Console.SetOut(console);
        }

        Assert.Equal("Hello, Ada!", output.ToString().Trim());
    }

    // Without the edge, compiling fails with the error the page shows.
    [Fact]
    public void CompilingWithoutTheEdgeFails()
    {
        var builder = new StateGraph<Greeting>()
            .AddNode("Compose", state => state with { Message = $"Hello, {state.Name}" })
            .AddNode("Punctuate", state => state with { Message = state.Message + "!" });

        var error = Assert.Throws<GraphCompilationException>(() => builder.Compile());
        Assert.Equal("Datalium.Strata.Validation.GraphCompilationException", error.GetType().FullName);
        Assert.Equal(
            "Graph compilation failed:\n[STRATA_ERR_ORPHAN] Node 'Punctuate' is isolated and unreachable from the entry point 'Compose'.",
            error.Message.ReplaceLineEndings("\n"));
    }
}

// docs:show
public sealed record Greeting(string Name, string Message);
// docs:end start-here/quickstart/program

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class QuickstartConsole
{
    public const string Name = "Quickstart console";
}
