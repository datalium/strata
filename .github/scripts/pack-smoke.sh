#!/usr/bin/env bash
# Packs the .NET packages and proves they work (design/36-Packaging-And-Versioning.md, "Proving a package
# works"): the solution makes exactly Strata's six packages at the repository's version, each with its
# licence, readme, XML docs and symbols; the engine's package carries the source generator, and the others
# let it through; and an application that references only the packed hosting, AI and MCP packages builds and
# runs.
#
# Leaves the packages in dotnet/artifacts/packages, which is what a release pushes.
set -euo pipefail
# grep reads here-strings, not pipes: `grep -q` exits at its first match, and with pipefail the writer's broken
# pipe would fail a check that passed.

root=$(cd "$(dirname "$0")/../.." && pwd)
packages="$root/dotnet/artifacts/packages"
smoke="$root/dotnet/tests/PackageSmoke"
version=$("$root/.github/scripts/versions.sh")

fail() {
    echo "pack-smoke: $1" >&2
    exit 1
}

rm -rf "$packages" "$smoke/obj" "$smoke/bin"
dotnet pack "$root/dotnet/Datalium.Strata.sln" --configuration Release --output "$packages"

expected="Datalium.Strata Datalium.Strata.AI Datalium.Strata.Cloud Datalium.Strata.Hosting Datalium.Strata.Mcp Datalium.Strata.Storage"
made=$(cd "$packages" && ls ./*.nupkg | sed -e 's|^\./||' -e "s|\.$version\.nupkg$||" | sort | tr '\n' ' ' | sed 's/ $//')
[ "$made" = "$expected" ] || fail "expected the packages '$expected' at $version, and the solution made '$made'"

for id in $expected; do
    package="$packages/$id.$version.nupkg"
    [ -f "$packages/$id.$version.snupkg" ] || fail "$id has no symbol package"
    contents=$(unzip -Z1 "$package")
    for file in "$id.nuspec" README.md "lib/net10.0/$id.dll" "lib/net10.0/$id.xml"; do
        grep -qx "$file" <<<"$contents" || fail "$id has no $file"
    done
    nuspec=$(unzip -p "$package" "$id.nuspec")
    grep -q '<license type="expression">Apache-2.0</license>' <<<"$nuspec" || fail "$id doesn't state its licence"
    grep -q "<version>$version</version>" <<<"$nuspec" || fail "$id is not at $version"
    grep -q '<description>Package Description</description>' <<<"$nuspec" && fail "$id has no description"
    if [ "$id" != "Datalium.Strata" ]; then
        # The engine at the same version, with its analyzers let through to this package's consumers.
        dependency=$(grep '<dependency id="Datalium.Strata" ' <<<"$nuspec") || fail "$id doesn't depend on Datalium.Strata"
        grep -q "version=\"$version\"" <<<"$dependency" || fail "$id depends on another version of Datalium.Strata"
        grep -qi 'analyzers' <<<"$dependency" && fail "$id hides Datalium.Strata's source generator from its consumers"
    fi
done
grep -qx 'analyzers/dotnet/cs/Datalium.Strata.SourceGenerators.dll' <<<"$(unzip -Z1 "$packages/Datalium.Strata.$version.nupkg")" \
    || fail "Datalium.Strata doesn't contain the source generator"

dotnet run --project "$smoke" --configuration Release

echo "pack-smoke: six packages at $version, and an application built on them alone runs."
