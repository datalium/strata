# Datalium Strata

A type-safe, compiled framework for orchestrating AI agents as state graphs, for Rust and .NET.

**Strata `0.9.0` is a stable release.** No release removes or breaks a public API: one we replace is marked deprecated first, with what to use instead, and removed only in a later major version. The changelog says what each version adds and deprecates.

## Install

```bash
cargo add datalium-strata@0.9.0 datalium-strata-memory@0.9.0
```

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
