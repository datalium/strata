# Security Policy

## Supported versions

Strata is a preview. Only the latest preview gets security fixes.

| Version | Supported |
|---|---|
| Latest `0.9.0-preview.N` | Yes |
| Any earlier preview | No. Upgrade to the latest |

A fix ships as a new preview version on nuget.org, on crates.io and as a GitHub release. The [changelog](CHANGELOG.md) says what it fixes.

## Reporting a vulnerability

**Please don't report a security issue in a public issue, discussion or pull request.**

Report it privately, in one of two ways:

- **GitHub:** [report a vulnerability](https://github.com/datalium/strata/security/advisories/new) from the repository's Security tab. This is the preferred way.
- **Email:** [security@datalium.com](mailto:security@datalium.com).

Please include:

- what the issue is, and what an attacker could do with it
- the version, the language (C#, Rust or the CLI) and the package or crate
- the steps to reproduce it, or a proof of concept
- anything you know about a fix or a workaround

## What happens next

| When | What we do |
|---|---|
| Within 3 business days | Confirm we received your report |
| Within 10 business days | Tell you whether we accept it as a vulnerability, and how severe we think it is |
| Until it's fixed | Keep you updated at least every two weeks |
| When the fix is released | Publish a GitHub security advisory, and credit you unless you'd rather not be named |

Please give us a chance to release a fix before you disclose the issue publicly. We'll agree on a date with you.

## Scope

This policy covers the code in this repository: the NuGet packages `Datalium.Strata*`, the crates `datalium-strata*` and the `strata` CLI.

A vulnerability in a dependency is in scope if Strata uses it in a way that exposes you. Otherwise, please report it to the dependency's maintainers.
