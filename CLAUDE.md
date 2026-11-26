# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project Overview

Datalium Strata is an agentic AI orchestration framework with two engines, one in Rust (crates.io) and one in .NET/C# (NuGet), plus the `strata` CLI. Both engines are built, tested and benchmarked (`benchmarks/`).

Design decisions live in `design/`. Each design doc has an "Implementation Status" section where what was built differs from the original plan. Read it before trusting a doc's code snippets.

## Project Structure

```
strata/
├── cli/                    # Rust CLI binary (strata): graph validate, thread list/inspect, checkpoint log/diff/replay
├── rust/                   # Cargo workspace
│   ├── strata-core/        # Engine: graphs, execution, traits (crate datalium-strata)
│   ├── strata-macros/      # #[strata_tool], #[derive(StrataSchema)]
│   ├── strata-memory/      # Checkpoint, history and memory stores: in-memory, SQLite, Postgres, Redis
│   ├── strata-cloud/       # Strata Cloud checkpoint store, decisions client and worker
│   ├── strata-host/        # Hosting: drivers as tasks, settings, health probe
│   ├── strata-ai/          # Model calls, agents, embeddings, long-term memory helper
│   ├── strata-mcp/         # Strata's own MCP client: an MCP server's tools as Strata tools
│   └── strata-bench/       # Benchmarks (not published)
├── dotnet/
│   ├── Datalium.Strata.sln
│   ├── src/Datalium.Strata/                   # Engine
│   ├── src/Datalium.Strata.Storage/           # Checkpoint, history and memory stores
│   ├── src/Datalium.Strata.Cloud/             # Strata Cloud checkpoint store, decisions client and worker
│   ├── src/Datalium.Strata.Hosting/           # AddStrata(): registration, settings, hosted drivers, health, OpenTelemetry
│   ├── src/Datalium.Strata.AI/                # Model calls, agents, embeddings, long-term memory helper
│   ├── src/Datalium.Strata.Mcp/               # Strata's own MCP client: an MCP server's tools as Strata tools
│   ├── src/Datalium.Strata.SourceGenerators/  # Roslyn generator for [StrataTool], shipped inside Datalium.Strata
│   ├── benchmarks/Datalium.Strata.Benchmarks/ # BenchmarkDotNet; in the solution, not a package
│   ├── tests/Datalium.Strata.Tests/
│   └── tests/PackageSmoke/                    # An app that uses Strata only as packages; run by pack-smoke.sh, not in the solution
├── schema/                 # Shared JSON Schemas and fixtures that pin the cross-language wire format
├── benchmarks/             # README.md (how to run) and RESULTS.md (the last full run)
├── design/                 # Design docs, one per feature
├── examples/               # dotnet-agent (+ tests), rust-agent, docs-snippets (the stratamesh.dev code samples)
├── .github/scripts/        # versions.sh (the one version, checked), pack-smoke.sh (pack and prove the NuGet packages)
└── CHANGELOG.md            # One entry per version, with the wire formats it writes and reads
```

## Build & Test Commands

These match CI. The Rust commands run separately in `rust/`, `cli/`, `examples/rust-agent/` and `examples/docs-snippets/rust/` (four independent Cargo projects).

**Rust / CLI**
```bash
cargo fmt --all                                            # Format
cargo clippy --all-targets --all-features -- -D warnings   # Lint
cargo test --all-features                                  # Test
cargo build --release --bin strata                         # Build the CLI (in cli/)
```

**.NET**
```bash
dotnet restore dotnet/Datalium.Strata.sln
dotnet format dotnet/Datalium.Strata.sln --verify-no-changes   # Lint
dotnet build dotnet/Datalium.Strata.sln --configuration Release --no-restore
dotnet test dotnet/Datalium.Strata.sln --configuration Release --no-build
```

**Packaging** (`design/36-Packaging-And-Versioning.md`)
```bash
.github/scripts/versions.sh                                     # Print the version; fail if the manifests disagree
.github/scripts/pack-smoke.sh                                   # Pack the six NuGet packages, check them, run an app built on them alone
(cd rust && cargo package --workspace --exclude strata-bench)   # Package each published crate and build it from its package
```

Both workflows (`.github/workflows/rust-ci.yml`, `dotnet-ci.yml`) run on pull requests to `main`, on pushes to `main` and `v*` tags, and on `workflow_dispatch`. Every run checks the version and packs. A `v*` tag also publishes.

**Cross-engine** (`design/Arch-Specs/STRATA-SPEC-005.md`, "Conformance"): `.github/workflows/cross-engine-ci.yml` runs on the same triggers. Each engine writes databases for the other to migrate, read and resume, then the CLI reads them. Locally, with `docker compose up -d --wait`:
```bash
export STRATA_INTEROP_DIR=$PWD/cross-engine STRATA_TEST_POSTGRES_URL=postgres://strata:strata@localhost:5432/strata
STRATA_INTEROP_STEP=write sh -c '(cd rust && cargo test -p datalium-strata-memory --test cross_engine) && dotnet test dotnet/tests/Datalium.Strata.Tests --filter "FullyQualifiedName~CrossEngineTests"'
STRATA_INTEROP_STEP=read-older sh -c '(cd cli && cargo test --test cross_engine)'   # the CLI reads both version 1 databases read-only, before any engine migrates them
STRATA_INTEROP_STEP=read  sh -c '(cd rust && cargo test -p datalium-strata-memory --test cross_engine) && dotnet test dotnet/tests/Datalium.Strata.Tests --filter "FullyQualifiedName~CrossEngineTests" && (cd cli && cargo test --test cross_engine)'
```
Without `STRATA_INTEROP_DIR`, each engine's test reads only what it wrote, as part of its ordinary test run.

**Benchmarks** (`design/37-Benchmarks.md`, `benchmarks/README.md`)
```bash
(cd rust && cargo bench -p strata-bench)                                                       # Rust microbenchmarks
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- --filter '*'   # .NET microbenchmarks
(cd rust && cargo run --release -p strata-bench -- --out ../rust-profile.json)                 # Rust profile
dotnet run -c Release --project dotnet/benchmarks/Datalium.Strata.Benchmarks -- profile        # .NET profile
(cd rust && cargo test -p strata-bench --benches && cargo run -p strata-bench -- --quick)      # Everything once, measuring nothing (what CI runs)
```

## Rules

- **Strict parity between the engines.** Every feature is built in both Rust and .NET, with the same behaviour and the same names where each language allows. A change to one engine comes with the same change to the other in the same PR.
- **One wire format.** Checkpoints, graph definitions, history records and memory records follow the JSON Schemas in `schema/`, so either engine and the CLI can read what the other wrote. The shared fixtures in `schema/fixtures/` are tested by both engines. Wire formats only grow: a new field gets a new minor `schema_version`, and old versions stay readable (`schema/README.md`, "Compatibility").
- **One version** for every package, crate and the CLI. `versions.sh` checks it.
- **Update the docs with the code.** A behaviour change updates its `design/` doc (including its Implementation Status) and adds a `CHANGELOG.md` entry, with the wire formats if they changed.
- **The stratamesh.dev samples are tests.** `examples/docs-snippets/` holds every code sample on the docs site, as regions inside tests (its `README.md`). An API change that breaks one fixes it in the same PR.
