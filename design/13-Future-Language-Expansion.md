# Future Language Expansion

This document captures the evaluation of additional language targets beyond the initial Rust + C# implementation. It exists to give future business and technical discussions a written reference point, so the "should we add language X" conversation doesn't restart from zero each time it comes up.

**Status:** Exploratory / not scheduled. No language beyond Rust and C# is committed to the roadmap. This document should be revisited once the Rust and .NET implementations reach parity and a stable 1.0 release of the core engine.

## Why Rust + C# First

Strata's core differentiator against LangChain/LangGraph is being a **type-safe, compiled** framework with **zero runtime reflection** in the hot path (see [STRATA-SPEC-003](Arch-Specs/STRATA-SPEC-003.md)). Rust (`#[strata_tool]` proc-macros) and C# (Roslyn source generators) were chosen because both can generate JSON-Schema manifests and static dispatch tables **at compile time**. Any future language candidate should be measured against this same bar, not just "is it fast."

## Evaluation Criteria

A candidate language is judged against the same four pillars that define the framework:

| Criterion | Requirement |
|---|---|
| **Compile-time metaprogramming** (SPEC-003) | Must generate tool schemas/dispatch tables at build time without runtime reflection |
| **Graph validation** (SPEC-004) | Must support the static analysis needed to run Tarjan's SCC check at `.Compile()` time |
| **Streaming concurrency** (SPEC-001) | Needs a mature async/channel primitive equivalent to `tokio::mpsc` or `System.Threading.Channels` |
| **Ecosystem maturity** | A stable package registry and mature drivers for SQLite/Postgres/Redis (for `strata-memory` equivalents) |
| **Distinct audience** | Should reach developers Rust/C# don't already cover, not duplicate them |

## Candidate Languages

### Go

- **Metaprogramming fit:** Weak. No macro system; generics are comparatively thin. Achieving SPEC-003's "no reflection in the hot path" would likely require `go:generate` + reflection, which undercuts the core value proposition.
- **Streaming fit:** Strong. Goroutines and native channels map naturally onto the SPEC-001 ring-buffer design.
- **Ecosystem:** Mature (cloud/infra tooling, solid DB drivers).
- **Audience:** Large and distinct — cloud/infra/DevOps developers who are unlikely to reach for Rust or C#.
- **Verdict:** Best "someday" candidate for a full native implementation, but only once the reflection-avoidance problem has a real answer (e.g., a code-generation tool ahead of build).

### Zig

- **Metaprogramming fit:** Strong, arguably the best fit of the four. `comptime` does genuine compile-time code generation and static dispatch, philosophically closer to Rust's proc-macros than anything else evaluated.
- **Streaming fit:** Weak. Zig has no built-in async/await story (removed it in recent releases); the SPEC-001 channel architecture would need to be hand-rolled.
- **Ecosystem:** Immature. Pre-1.0 with breaking changes between releases; no package registry with the maturity of Cargo or NuGet; thin DB driver support.
- **Audience:** Small, systems-programming niche that overlaps heavily with Rust's audience.
- **Verdict:** Technically compelling but too unstable to build a product on right now. Revisit after Zig reaches 1.0 and its ecosystem fills in.

### C++

- **Metaprogramming fit:** Strong. Templates + `constexpr` can achieve compile-time codegen comparable to Rust's macros.
- **Streaming fit:** Moderate. No standard-library channel primitive; would need Boost.Asio or a hand-rolled event loop.
- **Ecosystem:** Fragmented. No unified package/build story (CMake + vcpkg/conan vs. Cargo/NuGet's single toolchain).
- **Safety:** No memory-safety guarantees — this is the disqualifying issue. It directly contradicts the "type-safe" half of Strata's positioning.
- **Audience:** Not distinct — largely the same systems-programming audience Rust already serves, without Rust's safety story.
- **Verdict:** Not recommended. Adds surface area without adding a new value proposition or a new audience.

### Java

- **Metaprogramming fit:** Good. Annotation processors are a reasonable analogue to Roslyn source generators for SPEC-003.
- **Streaming fit:** Good. Virtual threads (Project Loom) fit the SPEC-001 streaming model well.
- **Ecosystem:** Mature (Maven/Gradle, mature Postgres/Redis drivers).
- **Audience:** Distinct — JVM-shop enterprises that won't adopt .NET or Rust but want the same compiled/type-safe guarantees.
- **Verdict:** The most defensible candidate for a genuine fourth native implementation, if and when the business case calls for deeper enterprise/JVM reach. Still a full engine to build and keep in sync with the Rust/C# schema contract — not a small undertaking.

## Alternative Strategy: Bindings Instead of a Full Reimplementation

Before committing to any new native implementation, consider exposing **bindings to the existing Rust core** instead:

- **Python** via [PyO3](https://pyo3.rs/) — this reaches LangChain's actual user base directly, without re-solving compile-time metaprogramming or memory safety in a new systems language.
- **Node/TypeScript** via [napi-rs](https://napi.rs/) — reaches the JS/TS agent-tooling ecosystem the same way.

This is likely the **highest-leverage move for growth**: it puts Strata in front of the exact audience currently using LangChain/LangGraph, while keeping all safety- and performance-critical logic centralized in `strata-core`. It sidesteps every ecosystem/metaprogramming tradeoff above, since Python/TypeScript callers consume a compiled Rust binary rather than needing their own compile-time codegen story. The tradeoff: bindings expose a subset of the API surface and inherit Rust's release cadence, so they're a complement to the core engines, not a peer implementation.

## Summary Table

| Language | Metaprogramming | Streaming | Ecosystem | Distinct Audience | Recommendation |
|---|---|---|---|---|---|
| Go | Weak | Strong | Mature | Yes (cloud/infra) | Reconsider later, once codegen story improves |
| Zig | Strong | Weak | Immature | No (overlaps Rust) | Reconsider after Zig 1.0 |
| C++ | Strong | Moderate | Fragmented | No (overlaps Rust) | Not recommended |
| Java | Good | Good | Mature | Yes (JVM enterprise) | Strongest native 4th-language candidate |
| Python/TS (bindings) | N/A — consumes Rust core | N/A | Mature | Yes (LangChain's own audience) | **Highest-leverage near-term move** |

## Recommendation

1. Do not add a language until Rust and C# reach parity and a stable 1.0.
2. When ready to expand reach, prioritize **Python/TypeScript bindings on top of `strata-core`** over a new native implementation — it's the fastest path to the largest, most relevant audience.
3. If a full native fourth language is later justified by business demand, **Java** is the strongest candidate of those evaluated.
4. Revisit **Go** if a lightweight compile-time codegen approach (avoiding reflection) becomes practical.
5. **C++** is not recommended under the current positioning. **Zig** should be revisited only after it reaches 1.0 stability.
