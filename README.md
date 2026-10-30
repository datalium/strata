# Datalium Strata

[![Public Preview](https://img.shields.io/badge/status-public%20preview-orange)](#why-strata-is-a-preview)
[![NuGet](https://img.shields.io/nuget/vpre/Datalium.Strata?label=nuget)](https://www.nuget.org/packages/Datalium.Strata.Hosting)
[![crates.io](https://img.shields.io/crates/v/datalium-strata?label=crates.io)](https://crates.io/crates/datalium-strata)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Datalium Strata is a type-safe, compiled, open-source, high-performance, and native agentic AI orchestration framework targeting the Rust and .NET/C# ecosystems, built for high-concurrency production workloads.

Strata is in **public preview**. The current version is `0.9.0-preview.1`. Read [Why Strata is a preview](#why-strata-is-a-preview) and the [Known limitations](https://stratamesh.dev/reference/known-limitations/) before you build on it.

**Documentation:** [stratamesh.dev](https://stratamesh.dev) · **Quickstart:** [stratamesh.dev/start-here/quickstart](https://stratamesh.dev/start-here/quickstart/)

## What it does

- **Your graph is checked before it runs.** An unreachable node or an unguarded cycle fails `Compile()`, so it fails your tests and CI, not a run in production. The graph structure and state types are checked; the LLM is still free to be wrong.
- **It survives a crash.** Strata can checkpoint after every node. Kill the process mid-run, restart it, and the thread carries on from the last node it committed.
- **Human approval that survives restarts and costs nothing while it waits.** A paused thread is saved to the store and holds nothing in your process until it resumes ([measured](benchmarks/RESULTS.md) with the SQLite store).
- **Agents and model calls built in.** In .NET on any `IChatClient` from Microsoft.Extensions.AI; in Rust with clients for OpenAI-compatible servers and Anthropic.
- **No Python sidecar.** A NuGet package or a crate in the service you already run.
- **Tool schemas generated from your code at compile time,** with `[StrataTool]` in C# and `#[strata_tool]` in Rust.
- **One wire format and one CLI for both languages.** Either engine reads what the other wrote, and `strata` inspects, diffs and replays checkpoints from both.

Performance: method, hardware and raw data are in [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

## Install

**.NET** (.NET 10). Previews aren't picked by default, so give the version:

```bash
dotnet add package Datalium.Strata.Hosting --version 0.9.0-preview.1
```

`Datalium.Strata.Hosting` brings the engine, the stores, the Strata Cloud client and the AI package.

**Rust** (1.88 or later, with tokio):

```bash
cargo add datalium-strata@0.9.0-preview.1 datalium-strata-memory@0.9.0-preview.1
```

**The `strata` CLI:** download the binary for your platform from the [latest release](https://github.com/datalium/strata/releases) (`strata-linux-x64`, `strata-macos-x64` or `strata-windows-x64.exe`) and check it against `SHA256SUMS.txt`. There is no ARM64 build yet.

The [Installation](https://stratamesh.dev/start-here/installation/) page lists every package and crate.

## A first graph

Two nodes joined by one edge, compiled, run and read back.

**C#** (`Program.cs`):

```csharp
using Datalium.Strata.Graph;
using Datalium.Strata.Interruption;
using Datalium.Strata.Storage;

var graph = new StateGraph<Greeting>()
    .AddNode("Compose", state => state with { Message = $"Hello, {state.Name}" })
    .AddNode("Punctuate", state => state with { Message = state.Message + "!" })
    .AddEdge("Compose", "Punctuate")
    .Compile(); // checks the graph before anything runs

var store = new InMemoryCheckpointStore();
var result = await graph.RunAsync("greeting-1", new Greeting("Ada", ""), store);

if (result is GraphExecutionResult<Greeting>.Completed completed)
{
    Console.WriteLine(completed.State.Message); // Hello, Ada!
}

public sealed record Greeting(string Name, string Message);
```

**Rust** (`src/main.rs`, with `serde` and `tokio` as dependencies):

```rust
use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Greeting {
    name: String,
    message: String,
}

#[tokio::main]
async fn main() {
    let graph = StateGraph::<Greeting>::new()
        .add_node("Compose", |s: Greeting| Greeting { message: format!("Hello, {}", s.name), ..s })
        .add_node("Punctuate", |s: Greeting| Greeting { message: format!("{}!", s.message), ..s })
        .add_edge("Compose", "Punctuate")
        .compile() // checks the graph before anything runs
        .expect("the graph is valid");

    let store = Arc::new(InMemoryCheckpointStore::new());
    let initial = Greeting { name: "Ada".into(), message: String::new() };

    if let GraphExecutionResult::Completed(state) = graph.run("greeting-1", initial, store).await {
        println!("{}", state.message); // Hello, Ada!
    }
}
```

Delete the `AddEdge` / `add_edge` line and compiling fails in both languages with the same error:

```text
[STRATA_ERR_ORPHAN] Node 'Punctuate' is isolated and unreachable from the entry point 'Compose'.
```

The [Quickstart](https://stratamesh.dev/start-here/quickstart/) explains each line. The [support ticket agent tutorial](https://stratamesh.dev/tutorials/support-ticket-agent/) adds a tool call and a pause for approval.

## Demos

29 demos in each language, each a small runnable project: tools, approvals, durable execution, agents, model routes, long-term memory, hosting and cross-language checkpoints.

- .NET: [datalium/strata-demos-dotnet](https://github.com/datalium/strata-demos-dotnet)
- Rust: [datalium/strata-demos-rust](https://github.com/datalium/strata-demos-rust)

Most run without an API key. The ones that need a model say so at the top of their README.

## Why Strata is a preview

The engines are built and tested in both languages, and the packages are published. The version is still a preview because three things that 0.9.0 requires aren't done:

- **A formal checkpoint schema.** `schema/` is versioned and migrations exist, but the spec that makes it normative, with a tested migration, isn't written yet.
- **A load report from a real application.** The benchmark suite ships now. Strata was built alongside StrataOps, an autonomous SRE system built on Strata (not public yet). Its large-scale load test, run against a simulator, isn't finished.
- **StrataOps building from these packages** instead of from source.

After 0.9.0, 1.0 adds a promise of API stability, outside teams running Strata in production, and a checkpoint schema that stays stable through a release.

Until then, the API can change: while the version is `0.x`, a minor version may break it, and a preview promises nothing between previews. The [changelog](CHANGELOG.md) says what changed in each version. The [Known limitations](https://stratamesh.dev/reference/known-limitations/) page lists what isn't built yet, and what to do in the meantime.

## Repository layout

| Folder | What it holds |
|---|---|
| `dotnet/` | The .NET engine, stores, hosting, Strata Cloud client and AI package, with their tests |
| `rust/` | The Rust crates, with their tests and benchmarks |
| `cli/` | The `strata` CLI |
| `schema/` | The JSON Schemas and fixtures both engines are tested against |
| `examples/` | A small agent in each language |
| `benchmarks/` | How to run the benchmarks, and the last full results |
| `design/` | One design doc per feature |

## Get involved

- **Questions and ideas:** [GitHub Discussions](https://github.com/datalium/strata/discussions)
- **Bugs and feature requests:** [GitHub Issues](https://github.com/datalium/strata/issues)
- **Contributing:** [CONTRIBUTING.md](CONTRIBUTING.md). Every commit is signed off under the DCO
- **Security issues:** [SECURITY.md](SECURITY.md). Please don't open a public issue
- **Code of conduct:** [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## License

Apache License 2.0. See [LICENSE](LICENSE). Strata is open source under Apache 2.0, and it stays that way.

Datalium Strata · [stratamesh.ai](https://stratamesh.ai)
