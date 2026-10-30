# Contributing to Datalium Strata

Thanks for helping. This page covers how to build and test Strata, the rules every change follows, and how to send a pull request.

Strata is a preview (see the [README](README.md#why-strata-is-a-preview)). The API can still change, so for anything bigger than a bug fix, open an issue or a [discussion](https://github.com/datalium/strata/discussions) first. That way we can agree on the design before you write the code.

## What you need

- The **.NET 10 SDK**.
- **Rust** 1.88 or later, with `rustfmt` and `clippy` (`rustup component add rustfmt clippy`).
- **Docker**, only if you want to run the Postgres and Redis store tests.

## Build and test

These are the commands CI runs. The Rust code is three separate Cargo projects: `rust/` (the crates), `cli/` (the `strata` CLI) and `examples/rust-agent/`. Run the Rust commands in each one you changed.

**Rust and the CLI**

```bash
cargo fmt --all                                            # format
cargo clippy --all-targets --all-features -- -D warnings   # lint
cargo test --all-features                                  # test
```

**.NET**

```bash
dotnet restore dotnet/Datalium.Strata.sln
dotnet format dotnet/Datalium.Strata.sln                   # format (CI runs it with --verify-no-changes)
dotnet build dotnet/Datalium.Strata.sln --configuration Release --no-restore
dotnet test dotnet/Datalium.Strata.sln --configuration Release --no-build
```

**The Postgres and Redis store tests** skip unless you give them a database. To run them:

```bash
docker compose up -d --wait
export STRATA_TEST_POSTGRES_URL=postgres://strata:strata@localhost:5432/strata
export STRATA_TEST_REDIS_URL=redis://localhost:6379
(cd rust && cargo test --all-features) && dotnet test dotnet/Datalium.Strata.sln
docker compose down
```

**Packaging**, if you changed a project file, a `Cargo.toml` or the version:

```bash
.github/scripts/versions.sh                                     # every manifest has the same version
.github/scripts/pack-smoke.sh                                   # packs the NuGet packages and runs an app built on them alone
(cd rust && cargo package --workspace --exclude strata-bench)   # each crate builds from its package
```

How to run the benchmarks is in [benchmarks/README.md](benchmarks/README.md).

## The rules every change follows

- **Both engines change together.** Every feature is built in both Rust and .NET, with the same behaviour and, where each language allows, the same names. A change to one engine comes with the same change to the other, in the same pull request. If you only know one of the languages, open the pull request anyway and say so: a maintainer can help with the other half.
- **One wire format.** Checkpoints, graph definitions, history records and memory records follow the JSON Schemas in `schema/`, so either engine and the CLI can read what the other wrote. Both engines are tested against the shared fixtures in `schema/fixtures/`. Wire formats only grow: a new field gets a new minor `schema_version`, and old versions stay readable. See `schema/README.md`, "Compatibility".
- **One version** for every package, crate and the CLI. `versions.sh` checks it. Don't change the version in a pull request; maintainers do it when they release.
- **Docs change with the code.** A change in behaviour updates its design doc in `design/` (including its "Implementation Status" section) and adds an entry to [CHANGELOG.md](CHANGELOG.md), with the wire formats if they changed.
- **Tests come with the change.** A bug fix gets a test that fails without it.

## Sending a pull request

1. Fork the repository and create a branch from `main`.
2. Make your change, with tests, in both engines.
3. Run the formatters, linters and tests above.
4. Sign off every commit (see below).
5. Open a pull request against `main` and fill in the template.

Keep a pull request to one change. Commit messages are short and in the imperative: "Add …", "Fix …", "Remove …".

CI must pass before a pull request is merged. A maintainer reviews every pull request.

## Sign off your commits (DCO)

Strata uses the [Developer Certificate of Origin](https://developercertificate.org/) (DCO), not a contributor license agreement. By signing off a commit, you certify that you wrote the change, or otherwise have the right to submit it under the project's license.

Sign off with `-s`:

```bash
git commit -s -m "Fix the retry delay in the Redis store"
```

This adds a line with your name and email to the commit message:

```text
Signed-off-by: Your Name <you@example.com>
```

Use your real name and an email address you can be reached at. Forgot to sign off? Fix the last commit with `git commit --amend -s --no-edit`, or every commit on your branch with `git rebase --signoff main`, then force-push the branch.

## Reporting bugs and asking for features

Open an [issue](https://github.com/datalium/strata/issues/new/choose) and pick the bug or feature template. For a question, use [Discussions](https://github.com/datalium/strata/discussions) instead.

**Security issues** are different: don't open a public issue. Follow [SECURITY.md](SECURITY.md).

## Code of conduct

Everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md).

## License

Strata is licensed under the [Apache License 2.0](LICENSE). Your contributions are licensed under it too.
