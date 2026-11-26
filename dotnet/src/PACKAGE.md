# Datalium Strata

A type-safe, compiled framework for orchestrating AI agents as state graphs, for .NET and Rust.

**Strata `0.9.0` is a stable release.** No release removes or breaks a public API: one we replace is marked deprecated first, with what to use instead, and removed only in a later major version. The changelog says what each version adds and deprecates.

## Install

```bash
dotnet add package Datalium.Strata.Hosting --version 0.9.0
```

| Package | What it is |
|---|---|
| `Datalium.Strata` | The engine: typed state graphs, compile-time validation, streaming, human-in-the-loop interrupts, tools generated at compile time (`[StrataTool]`), durable execution. Includes the source generator. |
| `Datalium.Strata.Storage` | Checkpoint, history and long-term memory stores: in-memory, SQLite, Postgres, Redis. |
| `Datalium.Strata.Cloud` | The Strata Cloud checkpoint store, decisions client and decision worker. |
| `Datalium.Strata.Hosting` | `services.AddStrata()`: registration, settings from configuration, the driver hosted services, health checks, OpenTelemetry wiring. Depends on the other four. |
| `Datalium.Strata.AI` | Model calls and agents from a node: any `Microsoft.Extensions.AI` `IChatClient`, streamed into the run, observed as run events and GenAI telemetry, provider faults retried with their `Retry-After`; the model node; `StrataAgent`, the model ↔ tool loop through a `ToolRegistry`, with typed answers validated and repaired; model routes; embeddings and the long-term memory helper. |
| `Datalium.Strata.Mcp` | Strata's own Model Context Protocol client: an MCP server's tools, over stdio or Streamable HTTP, as Strata tools in a `ToolRegistry`. Reference it beside `Datalium.Strata.Hosting`. |

An application usually references `Datalium.Strata.Hosting` alone: it brings the engine, the stores, the Cloud client and the AI package, and the source generator with them. `Datalium.Strata.Mcp` is referenced on its own, by an application that uses MCP servers.

## Links

- Documentation: https://stratamesh.dev
- Source and changelog: https://github.com/datalium/strata
- Known limitations: https://stratamesh.dev/reference/known-limitations/
- Contact: strata@datalium.com
- Security issues: see SECURITY.md in the repository. Please don't email them to the contact address
