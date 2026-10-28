# Datalium Strata

A type-safe, compiled framework for orchestrating AI agents as state graphs, for Rust and .NET.

**This is a preview.** The API can still change between versions; the changelog says what did.

## Install

```bash
cargo add datalium-strata@0.9.0-preview.1 datalium-strata-memory@0.9.0-preview.1
```

A preview version is only picked when you name it.

| Crate | What it is |
|---|---|
| `datalium-strata` | The engine: typed state graphs, compile-time validation, streaming, human-in-the-loop interrupts, durable execution, tools. |
| `datalium-strata-macros` | `#[strata_tool]` and `#[derive(StrataSchema)]`, re-exported by the engine. |
| `datalium-strata-memory` | Checkpoint, history and long-term memory stores: in-memory, SQLite, Postgres, Redis. |
| `datalium-strata-cloud` | The Strata Cloud checkpoint store, decisions client and decision worker. |
| `datalium-strata-host` | Driver loops, settings and a store health probe for running Strata in a service. |
| `datalium-strata-ai` | Model calls from a node: a `ChatClient` trait with OpenAI-compatible and Anthropic clients, streamed into the run, observed as run events and GenAI telemetry, provider faults retried with their `Retry-After`; the model node; `Agent`, the model ↔ tool loop with typed answers; model routes; embeddings and the long-term memory helper. |

The crates share one version and are released together.

## Links

- Documentation: https://stratamesh.dev
- Source and changelog: https://github.com/datalium/strata
- Known limitations: https://stratamesh.dev/reference/known-limitations/
- Contact: strata@datalium.com
- Security issues: see SECURITY.md in the repository. Please don't email them to the contact address
