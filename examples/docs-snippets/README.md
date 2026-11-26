# Docs snippets

The code samples on [stratamesh.dev](https://stratamesh.dev). Each one is a region inside a test, so CI compiles every sample and runs it (strata-management ADR-0001, rule 2). The docs site copies the regions from a release tag.

| Folder | What it is | Run it |
|---|---|---|
| `dotnet/` | An xUnit project in `dotnet/Datalium.Strata.sln` | `dotnet test examples/docs-snippets/dotnet` |
| `rust/` | A crate with no public items, only tests | `cd examples/docs-snippets/rust && cargo test` |
| `inventory-mcp/` | The MCP server the C# samples for `/guides/tools/mcp-servers/` connect to. Not a sample. The Rust samples use its twin, the crate's `inventory-mcp` example | Built by `dotnet test examples/docs-snippets/dotnet` |

Files follow the docs site's pages: the samples for `/concepts/graphs/` are in `dotnet/Concepts/Graphs.cs` and `rust/src/concepts/graphs.rs`.

## Regions

A region is the lines between two comments:

```csharp
// docs:begin concepts/graphs/branching
var graph = new StateGraph<OrderState>()
    .AddNode("CalculateSubtotal", state => state with { Subtotal = state.Quantity * state.UnitPrice })
    // docs:hide ... the other nodes
    .AddNode("ApplyDiscount", ...)
    // docs:show
    .SetEntryPoint("CalculateSubtotal")
    .Compile();
// docs:end concepts/graphs/branching
```

- **The name** is the page's path followed by a name for the sample: lowercase words and dashes, joined by `/`. A page shows it with `<Snippet file="concepts/graphs/branching.cs" />`, the name plus the file's extension. So the C# and the Rust sample for one place on a page share a name.
- **`docs:hide` and `docs:show`** leave lines out of the sample. The text after `docs:hide` replaces them, as a comment (`// ... the other nodes`). Without text, the lines go without a trace.
- **A `docs:show` indented deeper than its `docs:hide`** moves the lines after it left by the difference. That is how a sample shows `using` lines from the top of the file followed by code inside a test method:

  ```csharp
  // docs:begin guides/tools/define-tools/register
  using Datalium.Strata.Tools;

  // docs:hide
  namespace Datalium.Strata.DocsSnippets.Guides.Tools.DefineTools;

  public class DefineToolsSamples
  {
      [Fact]
      public void Register()
      {
          // docs:show
          var registry = new ToolRegistry();
          // docs:end guides/tools/define-tools/register
          Assert.NotNull(registry);
      }
  }
  ```

  The sample is the `using` line, a blank line, and `var registry = new ToolRegistry();`, all at the left margin.
- **The code around a region** sets the sample up and checks what it does. Put the asserts outside the region, so the sample shows only what a reader would write.
- Regions may nest or overlap. The marker lines never appear in a sample, and the indent every line shares is removed.
- `.toml`, `.sh` and `.csproj` files can hold regions too, with `#` and `<!-- -->` comments.
- **Whole files** go under `files/`, for samples that can't hold comments, such as JSON settings. Each file is a sample as it is, named by its path: `files/reference/configuration/appsettings.json` is `<Snippet file="reference/configuration/appsettings.json" />`. Load it from a test, so the test checks it: in C#, from `files/` next to the test assembly (the project copies it there), and in Rust with `include_str!` or from `concat!(env!("CARGO_MANIFEST_DIR"), "/../files/...")`.

The docs build fails when a page names a region that isn't here, and when a region is broken (no `docs:end`, a name used twice). `strata-docs-web`'s `scripts/snippets.ts` does the copying.

## When you change an API

A sample that no longer compiles fails CI here, in the same PR. Fix the sample with the change. If the page's text around it no longer fits, note it in the PR, so the docs can follow at the next release.
