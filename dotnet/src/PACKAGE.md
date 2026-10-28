# Datalium Strata

A type-safe, compiled framework for orchestrating AI agents as state graphs, for .NET and Rust.

**This is a preview.** The API can still change between versions; the changelog says what did.

## Install

```bash
dotnet add package Datalium.Strata.Hosting --version 0.9.0-preview.1
```

A preview version is only picked when you name it (or pass `--prerelease`).

| Package | What it is |
|---|---|
| `Datalium.Strata` | The engine: typed state graphs, compile-time validation, streaming, human-in-the-loop interrupts, tools generated at compile time (`[StrataTool]`), durable execution. Includes the source generator. |
| `Datalium.Strata.Storage` | Checkpoint, history and long-term memory stores: in-memory, SQLite, Postgres, Redis. |
| `Datalium.Strata.Cloud` | The Strata Cloud checkpoint store, decisions client and decision worker. |
| `Datalium.Strata.Hosting` | `services.AddStrata()`: registration, settings from configuration, the driver hosted services, health checks, OpenTelemetry wiring. Depends on the other four. |
| `Datalium.Strata.AI` | Model calls and agents from a node: any `Microsoft.Extensions.AI` `IChatClient`, streamed into the run, observed as run events and GenAI telemetry, provider faults retried with their `Retry-After`; the model node; `StrataAgent`, the model ↔ tool loop through a `ToolRegistry`, with typed answers validated and repaired; model routes; embeddings and the long-term memory helper. |

An application usually references `Datalium.Strata.Hosting` alone: it brings the other four packages, and the source generator with them.

## Links

- Documentation: https://stratamesh.dev
- Source and changelog: https://github.com/datalium/strata
- Known limitations: https://stratamesh.dev/reference/known-limitations/
- Contact: strata@datalium.com
- Security issues: see SECURITY.md in the repository. Please don't email them to the contact address
