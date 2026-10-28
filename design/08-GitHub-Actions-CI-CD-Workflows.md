## GitHub Actions CI/CD Workflows

To ensure code quality and seamless delivery, your repository will use two independent pipelines. These workflows automatically test, compile, package, and publish your crates, NuGet packages, and the native CLI binary.

> **Amended 2026-10-28, for the first public release:** this is the early design. The workflows in `.github/workflows/` are the source of truth and differ from it in several places: the .NET version, the packages and crates published, trusted publishing for nuget.org, and the order of the release steps. The CLI binaries below now have the names used on the GitHub release: `strata-linux-x64`, `strata-macos-x64` and `strata-windows-x64.exe`, with `SHA256SUMS.txt` beside them.

---

### 🦀 1. Rust and CLI Pipeline (`.github/workflows/rust-ci.yml`)

This workflow handles formatting, safety checks, unit testing, compiling the native CLI binary for three major operating systems (Windows, macOS, Linux), and publishing the crates to Crates.io on tag releases.

```yaml
name: Rust & CLI CI/CD

on:
    push:
        branches: [main]
        tags: ['v*']
    pull_request:
        branches: [main]

env:
    CARGO_TERM_COLOR: always

jobs:
    quality-and-test:
        name: Code Quality and Tests
        runs-on: ubuntu-latest
        steps:
            - uses: actions/checkout@v4

            - name: Setup Rust Toolchain
              uses: dtolnay/rust-toolchain@stable
              with:
                  components: clippy, rustfmt

            - name: Check Formatting
              run: cargo fmt --all -- --check

            - name: Run Clippy Lints
              run: cargo clippy --all-targets --all-features -- -D warnings

            - name: Run Engine & CLI Tests
              run: cargo test --all-features

    build-cli-artifacts:
        name: Build Cross-Platform CLI
        needs: quality-and-test
        if: startsWith(github.ref, 'refs/tags/v')
        runs-on: ${{ matrix.os }}
        strategy:
            matrix:
                include:
                    - os: ubuntu-latest
                      target: x86_64-unknown-linux-gnu
                      binary_name: strata
                      asset_name: strata-linux-x64
                    - os: macos-latest
                      target: x86_64-apple-darwin
                      binary_name: strata
                      asset_name: strata-macos-x64
                    - os: windows-latest
                      target: x86_64-pc-windows-msvc
                      binary_name: strata.exe
                      asset_name: strata-windows-x64.exe
        steps:
            - uses: actions/checkout@v4

            - name: Setup Rust Toolchain
              uses: dtolnay/rust-toolchain@stable
              with:
                  target: ${{ matrix.target }}

            - name: Compile CLI Binary
              run: cargo build --release --bin strata --target ${{ matrix.target }}

            - name: Name the Binary for the Release
              shell: bash
              run: |
                  mkdir -p dist
                  cp "target/${{ matrix.target }}/release/${{ matrix.binary_name }}" "dist/${{ matrix.asset_name }}"

            - name: Upload Binary Artifact
              uses: actions/upload-artifact@v4
              with:
                  name: ${{ matrix.asset_name }}
                  path: dist/${{ matrix.asset_name }}

    publish-rust:
        name: Publish to Crates.io & GitHub Releases
        needs: build-cli-artifacts
        runs-on: ubuntu-latest
        steps:
            - uses: actions/checkout@v4

            - name: Download All CLI Binaries
              uses: actions/download-artifact@v4
              with:
                  path: cli-binaries

            - name: Create GitHub Release
              uses: softprops/action-gh-release@v2
              with:
                  files: cli-binaries/**/*
              env:
                  GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

            - name: Publish to Crates.io
              run: |
                  cargo login ${{ secrets.CRATES_IO_TOKEN }}
                  cargo publish -p datalium-strata-macros
                  cargo publish -p datalium-strata-memory
                  cargo publish -p datalium-strata
```

### 🧑‍💻 2. .NET Pipeline (`.github/workflows/dotnet-ci.yml`)

This workflow builds the C# codebase, enforces formatting, executes xUnit tests, packages the code into standard .nupkg assets, and signs and ships them directly to NuGet.org.

```yaml
name: .NET Core CI/CD

on:
    push:
        branches: [main]
        tags: ['v*']
    pull_request:
        branches: [main]

jobs:
    build-and-test:
        name: .NET Verification & Testing
        runs-on: ubuntu-latest
        steps:
            - uses: actions/checkout@v4

            - name: Setup .NET SDK
              uses: actions/setup-dotnet@v4
              with:
                  dotnet-version: '8.0.x' # Tailor to your targeted LTS/STS framework

            - name: Restore Dependencies
              run: dotnet restore dotnet/Datalium.Strata.sln

            - name: Verify Code Formatting
              run: dotnet format dotnet/Datalium.Strata.sln --verify-no-changes

            - name: Build Solution (Release Configuration)
              run: dotnet build dotnet/Datalium.Strata.sln --configuration Release --no-restore

            - name: Run Test Engine
              run: dotnet test dotnet/Datalium.Strata.sln --configuration Release --no-build --verbosity normal

    publish-nuget:
        name: Package and Push to NuGet
        needs: build-and-test
        if: startsWith(github.ref, 'refs/tags/v')
        runs-on: ubuntu-latest
        steps:
            - uses: actions/checkout@v4

            - name: Setup .NET SDK
              uses: actions/setup-dotnet@v4
              with:
                  dotnet-version: '8.0.x'

            - name: Pack Library Projects
              run: dotnet pack dotnet/src/Datalium.Strata/Datalium.Strata.csproj --configuration Release --output nuget-packages

            - name: Push Packages to NuGet.org
              run: dotnet nuget push nuget-packages/*.nupkg --api-key ${{ secrets.NUGET_API_KEY }} --source https://nuget.org
```
