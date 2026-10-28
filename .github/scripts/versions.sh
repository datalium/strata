#!/usr/bin/env bash
# Prints Strata's version, and fails unless every manifest agrees on it
# (design/36-Packaging-And-Versioning.md, "The version check"): the .NET packages'
# (dotnet/Directory.Build.props), the crates' (rust/Cargo.toml, [workspace.package]) and the CLI's
# (cli/Cargo.toml). With --tag <tag>, also fails unless the tag is "v" followed by that version, so a tag that
# doesn't match the manifests publishes nothing.
#
#   versions.sh                 # 0.9.0-preview.1
#   versions.sh --tag v0.9.0    # fails: the manifests say 0.9.0-preview.1
#   versions.sh --prerelease    # exits 0 if the version has a pre-release label, 1 if not
set -euo pipefail

root=${STRATA_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}

fail() {
    echo "versions: $1" >&2
    exit 1
}

# The first <Version> element.
dotnet_version=$(sed -n 's|.*<Version>\(.*\)</Version>.*|\1|p' "$root/dotnet/Directory.Build.props" | head -n 1)

# The version key of a section of a Cargo manifest.
cargo_version() {
    awk -v section="[$2]" '
        /^\[/ { inside = ($0 == section) }
        inside && /^version[ \t]*=/ { gsub(/^version[ \t]*=[ \t]*"|"[ \t]*$/, ""); print; exit }
    ' "$1"
}
rust_version=$(cargo_version "$root/rust/Cargo.toml" "workspace.package")
cli_version=$(cargo_version "$root/cli/Cargo.toml" "package")

[ -n "$dotnet_version" ] || fail "dotnet/Directory.Build.props has no <Version>"
[ -n "$rust_version" ] || fail "rust/Cargo.toml has no [workspace.package] version"
[ -n "$cli_version" ] || fail "cli/Cargo.toml has no [package] version"
[ "$dotnet_version" = "$rust_version" ] \
    || fail "dotnet/Directory.Build.props says $dotnet_version and rust/Cargo.toml says $rust_version"
[ "$dotnet_version" = "$cli_version" ] \
    || fail "dotnet/Directory.Build.props says $dotnet_version and cli/Cargo.toml says $cli_version"

# The crates depend on their siblings at exactly this version: a release publishes them together.
while IFS= read -r line; do
    echo "$line" | grep -q "version = \"=$dotnet_version\"" \
        || fail "rust/Cargo.toml: a workspace crate isn't pinned to =$dotnet_version: $line"
done < <(grep -E '^datalium-strata[a-z-]* *= *\{' "$root/rust/Cargo.toml")

case "${1:-}" in
    "")
        echo "$dotnet_version"
        ;;
    --tag)
        [ "${2:-}" = "v$dotnet_version" ] \
            || fail "the tag is '${2:-}' and the manifests say $dotnet_version: a release of it is tagged v$dotnet_version"
        echo "$dotnet_version"
        ;;
    --prerelease)
        case "$dotnet_version" in
            *-*) exit 0 ;;
            *) exit 1 ;;
        esac
        ;;
    *)
        fail "unknown argument '$1'"
        ;;
esac
