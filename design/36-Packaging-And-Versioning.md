# Datalium Strata: Packaging and Versioning

Nobody can install Strata. An application that uses it references a sibling checkout by path and pins a commit. That works for one team on one machine, and it has four costs:

- **Nothing is versioned.** No project sets a version. The Rust crates say `0.1.0`; `dotnet pack` falls back to **`1.0.0`**. There are no tags and no changelog, so "which Strata is this application on" is a commit hash.
- **The release jobs would publish the wrong thing.** Both workflows publish on a `v*` tag. The .NET one packs only `Datalium.Strata`, at `1.0.0`, without its source generator, and pushes to a URL that isn't NuGet's. The Rust one publishes three of the five crates, in an order that can't work (the storage crate before the engine it depends on), from manifests whose path dependencies have no version, which crates.io refuses.
- **The source generator isn't in the package.** `[StrataTool]` needs `Datalium.Strata.SourceGenerators` at compile time. It is wired into `Datalium.Strata` as an analyzer for that project's own build, and nothing carries it to a consumer: every consuming project references the generator project itself (`Datalium.Strata.csproj` says so in a comment).
- **Nothing says what a version promises.** The wire formats have careful rules (`schema/README.md`, "Versioning"), but no document says which package version reads what, or in what order runtimes can be upgraded.

This document closes the gap in the engine: **SG-29**. The SG numbers refer to the gap log of StrataOps (not public yet). The gap names NuGet; under strict parity (StrataOps ADR 0003) the Rust crates get the same treatment, or a stated reason where they differ.

It is written as a blueprint before implementation. "Review decisions" lists the questions for review with the recommendation the build follows, and "Implementation Status" at the end records where the build diverged.

**What this document does not do is publish anything.** It makes the repository able to: a tag then publishes. Pushing a tag is the maintainer's act.

> **Amended 2026-10-28, for the first public release:** previews now publish to nuget.org and crates.io, like releases, and nothing goes to GitHub Packages. The decisions table, "Usage", "What a tag does" and the note on testing in "Implementation Status" say so. The review decisions and the rest of "Implementation Status" still describe the build of 2026-10-11. The AI packages were added after it (`design/44`), so there are now **five** NuGet packages (`Datalium.Strata.AI`) and **six** crates (`datalium-strata-ai`), where this document says four and five.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| The version | **One version for every package, crate and the CLI: `0.9.0-preview.1`.** Semantic versioning. `0.x` says the API can still break; `preview` says it is a pre-release. It is a preview of 0.9.0 because two of the evaluation's four gates for 0.9 are met and two remain (below). |
| Where it lives | **In the repository, in three manifests:** `dotnet/Directory.Build.props`, `rust/Cargo.toml` (`[workspace.package]`) and the CLI's own `cli/Cargo.toml`. A script checks they agree, and that a release tag is `v<version>`. Cargo can't take a version from the command line, so the tag can't be the source. |
| What is packaged (.NET) | **Four packages:** `Datalium.Strata`, `Datalium.Strata.Storage`, `Datalium.Strata.Cloud`, `Datalium.Strata.Hosting`. Nothing else in the solution is packable. |
| The source generator | **Inside `Datalium.Strata`**, under `analyzers/dotnet/cs`, and flowing to anything that depends on it, so a project that references only `Datalium.Strata.Hosting` can still write `[StrataTool]`. `Datalium.Strata.SourceGenerators` is not a package. |
| What is packaged (Rust) | **Five crates:** `datalium-strata-macros`, `datalium-strata`, `datalium-strata-memory`, `datalium-strata-cloud`, `datalium-strata-host`, each with the metadata crates.io requires and versioned dependencies on its siblings. The CLI is a binary on the GitHub release, not a crate. |
| Package contents | **XML docs, symbols (`.snupkg`) and Source Link** in the .NET packages; a short readme in each package and crate; the Apache-2.0 licence expression. |
| Previews | **A pre-release tag (`v0.9.0-preview.1`) publishes the NuGet packages to nuget.org and the crates to crates.io**, in dependency order, and makes a GitHub release with the CLI binaries, marked pre-release. *(Amended 2026-10-28. It was GitHub Packages only.)* |
| Releases | **A release tag (`v0.9.0`) publishes the same way**, with a GitHub release that isn't marked pre-release. Not before the maintainer decides to; nothing here creates one. |
| Rust previews | **On crates.io, like a release.** An application pins the exact preview: `datalium-strata = "=0.9.0-preview.1"`. A crates.io version can't be removed, only yanked. *(Amended 2026-10-28. It was the git tag.)* |
| Proving a package works | **CI packs on every pull request and uses the result:** a small project that references only the packed `Datalium.Strata.Hosting`, declares a `[StrataTool]`, and runs a graph; `cargo package --workspace`, which builds each crate from its packaged form. |
| Changelog | **`CHANGELOG.md`** at the root, newest first, one entry per version, written for someone upgrading: what breaks, what the wire formats need. |
| Compatibility | **A written policy** (below, and in `schema/README.md`): what a version may change, what every runtime reads, and the order to upgrade in. |
| Wire format | **None.** |

---

## Usage

```xml
<!-- An application, from nuget.org (or: dotnet add package Datalium.Strata.Hosting --prerelease) -->
<PackageReference Include="Datalium.Strata.Hosting" Version="0.9.0-preview.1" />
```

One reference is enough: the hosting package depends on the engine, the stores, the Cloud client and the AI package, and the engine's source generator comes with it.

```toml
# Cargo.toml, a preview, from crates.io. Pin it exactly: a preview promises nothing between previews.
datalium-strata = "=0.9.0-preview.1"
datalium-strata-memory = "=0.9.0-preview.1"

# a release
datalium-strata = "0.9"
```

Releasing, for the maintainer:

```bash
.github/scripts/versions.sh                 # prints the version; fails if the manifests disagree
# edit the version in the three manifests (and the crates' pins in rust/Cargo.toml), add the CHANGELOG entry, merge
git tag v0.9.0-preview.1 && git push origin v0.9.0-preview.1
```

---

## Semantics

### One version

Every package, every crate and the CLI carry the same version, and move together. A change to one package is a new version of all of them. That is simpler to reason about than a matrix, and the packages are not independent: the stores implement the engine's ports, and the wire formats are shared.

The version is `MAJOR.MINOR.PATCH`, with a pre-release label for previews (`-preview.N`):

- **`0.x`:** a minor version may break the API. The changelog says what. A patch version doesn't.
- **A preview** promises nothing between previews.
- **`0.9.0`** itself, without the label, waits for the evaluation's two remaining gates for it: benchmarks (SG-27), and a pre-release published on both registries. The second matters for where previews go (review question 3): a preview that only ever reaches GitHub Packages doesn't meet it.
- **`1.0.0`** is the stability promise the 2026-09-21 evaluation describes, and is not this document's.

### The version check

`.github/scripts/versions.sh` reads the version from `dotnet/Directory.Build.props`, from `rust/Cargo.toml` and from `cli/Cargo.toml`, and fails if they differ. Given `--tag <tag>` it also fails unless the tag is `v` followed by that version. Both workflows run it: on every pull request without a tag, on a tag with it. So a tag that doesn't match the manifests publishes nothing.

### The generator in the package

`Datalium.Strata.SourceGenerators.dll` is packed into `Datalium.Strata` under `analyzers/dotnet/cs`, where the compiler looks. A project with a `PackageReference` to `Datalium.Strata` gets the generator with no other reference.

NuGet doesn't pass a dependency's analyzers on by default: a package that depends on `Datalium.Strata` would hide the generator from its own consumers. `Datalium.Strata.Storage`, `.Cloud` and `.Hosting` therefore declare their dependency on the engine so that analyzers flow through, and an application that references only the hosting package can still write `[StrataTool]`.

Inside this repository nothing changes: projects reference each other by path, and a path reference doesn't carry analyzers, so the tests and the example keep their explicit analyzer reference.

### What a tag does

| Tag | NuGet packages | Crates | CLI binaries |
|---|---|---|---|
| `v0.9.0-preview.1` (has a `-`) | nuget.org | crates.io, in dependency order | GitHub release, marked pre-release |
| `v0.9.0` | nuget.org | crates.io, in dependency order | GitHub release |

*(Amended 2026-10-28. A preview tag used to publish to GitHub Packages only, and a release tag to GitHub Packages as well.)*

Before either, the version check runs with the tag, and the test jobs must pass.

### Proving a package works

Packing can break without any test noticing: a missing file, a dependency that didn't make it into the manifest, a generator that isn't where the compiler looks. So every pull request:

- **packs the four packages** into a folder and builds **`dotnet/tests/PackageSmoke`** against that folder as its only source of Strata. The project references `Datalium.Strata.Hosting` and nothing else of Strata's, declares a `[StrataTool]` (which only compiles if the generator arrived), registers Strata with `AddStrata()`, runs a graph and dispatches the tool. It exits non-zero on any surprise. It is not in the solution.
- runs **`cargo package --workspace`** in `rust/`, which packages each crate and builds it from the package, resolving its siblings from the packages just made.

### Compatibility

What a version may change, and what it may not:

1. **A record's `schema_version` changes only when its content needs it.** A writer writes the lowest version that can say what it has to say. A graph that uses nothing new writes what it always wrote, byte for byte.
2. **Every schema version is a superset of the one before.** A runtime that reads version N reads every version below it.
3. **A runtime refuses what it doesn't know.** A checkpoint, a definition or a database newer than the build is an error, never a guess.
4. **A package version reads everything every earlier version wrote.** Removing support for a schema version is a major version of the packages.
5. **Databases move forward only**, one recorded step at a time (`strata_schema_version`, `design/34`). A store migrates an older database when it opens it, or refuses it if told not to migrate.
6. **Redis has no migrations:** every writer to a prefix must be current, because the scripts that keep the index are part of the writer.

From which the upgrade order follows: **upgrade every reader before any writer starts using something new.** In practice, deploy the new version everywhere (workers, the API, the CLI) and only then turn on the feature that writes a new schema version (`EveryNode`, a graph identity, a wait).

The changelog's entry for a version lists the highest schema versions that version writes and reads, and the database versions it expects.

---

## Wire format

Unchanged.

---

## Migration

**This repository:** every project gets a version; `dotnet pack` on the solution makes four packages instead of failing on the generator project. The Rust crates take their version and metadata from the workspace.

**StrataOps:** replace the `ProjectReference`s to the sibling checkout, and the per-project analyzer reference, with `PackageReference`s at the preview version, from nuget.org. Its pin becomes a version.

**The release workflows** change what a tag does. No tag exists yet, so nothing that was published changes.

---

## Tests

- **`versions.sh`:** agrees on the checked-in manifests; fails when one is changed; accepts the matching tag and refuses another.
- **The packages:** each of the four is produced at the version, with its licence, readme, XML docs and symbols; `Datalium.Strata` contains the generator under `analyzers/dotnet/cs`; the others depend on `Datalium.Strata` at the same version with analyzers flowing; nothing else in the solution packs.
- **The smoke project** builds and runs against the packed packages alone.
- **`cargo package --workspace`** succeeds, and each crate's manifest has a description, a licence and a repository.
- **The CLI** reports the version.

---

## Review decisions

Not yet reviewed. The build follows each recommendation below; a different answer from review changes the build before merge.

1. **Which version?** **`0.9.0-preview.1`.** The 2026-09-21 evaluation's ladder put September's state at 0.3.0, before any of the work StrataOps drove. Its gate for 0.5.0 has been met since: a real application has run on a real model and on Postgres, its findings were fixed, and retry backoff, parallel group controls and the loop circuit breaker are built. Its gate for 0.9.x has four parts. Two are met: crash-durable checkpointing (`design/33`) and storage hardening (`design/34`). Two are not: benchmarks (SG-27), and a pre-release published on both registries. So this is a preview of 0.9.0, and 0.9.0 itself waits for those two.
2. **One version or one per package?** **One.**
3. **Where do previews go?** **GitHub Packages for NuGet, the git tag for Rust.** The gap says a GitHub Packages feed is enough. nuget.org and crates.io keep what they are given, so they get releases. This is the answer to change if the evaluation's "a published pre-release on both registries" is to be met by a preview: a pre-release tag would then also push to nuget.org and crates.io, which accept pre-release versions.
4. **Publish on a tag, or on every merge to `main`?** **On a tag.** A version per merge would need a version nobody chose, and a feed full of packages nobody asked for. StrataOps can still pin a commit between previews.
5. **The generator flowing through dependent packages.** **Yes.** Otherwise `[StrataTool]` compiles or doesn't depending on which Strata package an application happened to reference.
6. **XML docs in the packages.** **Yes,** with missing-comment warnings off: the public API is documented, and IntelliSense is where a consumer reads it.
7. **Does the README change?** **Only to say what this is:** a preview, at which version, and where to get it. The README is the project's own voice; this document doesn't rewrite it.

---

## Implementation Status

Built 2026-10-11, as specified above. Nothing was published: no tag was pushed and no package uploaded. The review questions are still open: the build follows each recommendation, and a different answer changes it before merge.

**What could not be tested here.** The pushes themselves: to GitHub Packages, to nuget.org and to crates.io, and the GitHub release. Everything up to the push runs on every pull request (the version check, packing, the package checks, the application built on the packages, `cargo package`). The push steps are a few lines each, and the first tag is their first run. Since the 2026-10-28 amendment a pre-release tag publishes to nuget.org and crates.io too, which keep what they get (a version can only be unlisted or yanked), so check everything before the first tag.

**Where it diverged, or filled a gap, and why:**

- **The version is in three files, not two.** The CLI is a Cargo project of its own, outside the Rust workspace, so it states the version itself. The script and a CLI test compare all three.
- **The check is also a test.** `cli/tests/version.rs` compares the three manifests and the crates' pins under `cargo test`, so a version changed in one place fails on a developer's machine, not only in the workflow. It also checks `strata --version`.
- **The crates pin each other exactly** (`=0.9.0-preview.1`), and the script checks the pins. The crates are released together, and a pre-release version only satisfies a requirement that names it.
- **Two more documentation warnings are off** in the packages besides the missing-comment one: `CS1573` (some parameters of a member described and not others) and `CS0419` (a reference to a method that has overloads). Turning XML docs on surfaced several dozen of those two, all of them correct to a reader. The three references that resolved to nothing were fixed, and such a reference is still a warning.
- **The packages' readme is one shared file** (`dotnet/src/PACKAGE.md`), and the crates' is `rust/README.md`: what each package is, and that this is a preview.
- **The smoke application unpacks its packages under its own `obj/`.** NuGet's machine-wide cache would otherwise keep serving the first build of a version after it has been packed again, and the check would pass on a stale package.
- **GitHub Packages gets no symbol packages.** It has no symbol server. nuget.org gets the `.snupkg` files with a release.
- **`cargo package` warns about a yanked crate** in the lock file (`yoke-derive`, through a dependency of a dependency). It doesn't fail the packaging, and `cargo update` is its own change.
- **The README gained a "Status" section** at the top (the version, where previews are, the changelog) and nothing else changed in it: it had no "production-grade" claim to soften.
- **The old release steps were wrong in ways that had never run:** the NuGet job packed one project at the default version `1.0.0` and pushed to `https://nuget.org`, which isn't the push endpoint; the crates job published the storage crate before the engine it depends on, and left two crates out.

**How it was built:**

- **Version and metadata:** `dotnet/Directory.Build.props` (the version, authors, licence, repository; `IsPackable=false` by default), `dotnet/src/Package.props` (what makes a project a package: docs, symbols, readme), imported by the four library projects, each with a `Description`. `rust/Cargo.toml`: `[workspace.package]` and `[workspace.dependencies]`; each crate inherits (`version.workspace = true`, …) and takes its siblings from the workspace. `cli/Cargo.toml`: the version, `publish = false`.
- **The generator in the package:** a packed `None` item in `Datalium.Strata.csproj`; `PrivateAssets="contentfiles;build"` on the engine reference in the other three, and on the hosting package's references to the stores and the Cloud client.
- **Scripts:** `.github/scripts/versions.sh` and `.github/scripts/pack-smoke.sh`. `dotnet/tests/PackageSmoke/` (project, `nuget.config`, `Program.cs`).
- **Workflows:** both check the version on every run (with the tag, on a tag). `dotnet-ci.yml` runs `pack-smoke.sh` in the test job, and on every `v*` tag, preview or not, pushes what it packed to nuget.org. `rust-ci.yml` runs `cargo package --workspace` in the test job, and on every `v*` tag publishes the six crates in order, then creates the GitHub release with the CLI binaries and `SHA256SUMS.txt` (a pre-release for a preview tag). Both publish jobs run in the `release` environment and use trusted publishing: no registry key or token is stored.
- **Documents:** `CHANGELOG.md`; `schema/README.md`, "Compatibility"; the README's "Status".

**Tests:**

- **`versions.sh`** on the checked-in manifests prints `0.9.0-preview.1`; it accepts the tag `v0.9.0-preview.1`, refuses `v0.9.0`, and says the version is a pre-release.
- **`pack-smoke.sh`:** four packages and four symbol packages at the version; each has its nuspec, readme, assembly and XML docs, states its licence and has a description; the three dependents depend on the engine at the same version without hiding its analyzers; the engine's package contains the generator; the smoke application builds on the packages alone, runs a graph through `AddStrata()` and dispatches a generated tool.
- **`cargo package --workspace`:** the five crates package and each builds from its package.
- **CLI:** `cli/tests/version.rs` (2).
- **Totals when this landed:** .NET 833, with Postgres and Redis running, and 2 skipped (the opt-in Cloud live tests), plus the example's 1. `rust/` 768 and `cli/` 80, with `STRATA_REQUIRE_BACKENDS=1` against live Postgres and Redis, and `examples/rust-agent` 1. `cargo fmt`, `clippy -D warnings` and `dotnet format --verify-no-changes` are clean.
- **Mutation checks** (each broken, the script run, then restored). All five fail with a message that names the fault:
  - The generator left out of the engine's package: `pack-smoke` says the package doesn't contain it.
  - A dependent package that hides the generator: `pack-smoke` names the package.
  - A fifth packable project: `pack-smoke` lists what the solution made against what it should.
  - A CLI at another version: `versions` names the two manifests that differ.
  - A crate pinned to another version of its sibling: `versions` quotes the line.

## Not built (deliberately out of scope)

- **Publishing.** No tag is pushed and no package is uploaded by this change.
- **Signing** the packages, and a `SECURITY.md` and support statement (the evaluation's 1.0 gate).
- **A package per merge** (a CI feed).
- **A NuGet package or a crate for the CLI** (`dotnet tool`, `cargo install`): it stays a binary on the release.
- **API-compatibility checking** between versions (`Microsoft.DotNet.ApiCompat`, `cargo-semver-checks`): worth adding before 0.9.0 itself, once there is a published preview to compare against.
- **Trimming and AOT annotations** on the packages.
- **Changelog generation** from commits: it is written by hand, for a reader.
