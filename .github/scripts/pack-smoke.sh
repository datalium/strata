#!/usr/bin/env bash
# Packs the .NET packages and proves they work (design/36-Packaging-And-Versioning.md, "Proving a package
# works"): the solution makes exactly Strata's five packages at the repository's version, each with its
# licence, readme, XML docs and symbols; the engine's package carries the source generator, and the others
# let it through; and an application that references only the packed hosting and AI packages builds and runs.
#
# Leaves the packages in dotnet/artifacts/packages, which is what a release pushes.
set -euo pipefail

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

expected="Datalium.Strata Datalium.Strata.AI Datalium.Strata.Cloud Datalium.Strata.Hosting Datalium.Strata.Storage"
made=$(cd "$packages" && ls ./*.nupkg | sed -e 's|^\./||' -e "s|\.$version\.nupkg$||" | sort | tr '\n' ' ' | sed 's/ $//')
[ "$made" = "$expected" ] || fail "expected the packages '$expected' at $version, and the solution made '$made'"

for id in $expected; do
    package="$packages/$id.$version.nupkg"
    [ -f "$packages/$id.$version.snupkg" ] || fail "$id has no symbol package"
    contents=$(unzip -Z1 "$package")
    for file in "$id.nuspec" README.md "lib/net10.0/$id.dll" "lib/net10.0/$id.xml"; do
        echo "$contents" | grep -qx "$file" || fail "$id has no $file"
    done
    nuspec=$(unzip -p "$package" "$id.nuspec")
    echo "$nuspec" | grep -q '<license type="expression">Apache-2.0</license>' || fail "$id doesn't state its licence"
    echo "$nuspec" | grep -q "<version>$version</version>" || fail "$id is not at $version"
    echo "$nuspec" | grep -q '<description>Package Description</description>' && fail "$id has no description"
    if [ "$id" != "Datalium.Strata" ]; then
        # The engine at the same version, with its analyzers let through to this package's consumers.
        dependency=$(echo "$nuspec" | grep '<dependency id="Datalium.Strata" ') || fail "$id doesn't depend on Datalium.Strata"
        echo "$dependency" | grep -q "version=\"$version\"" || fail "$id depends on another version of Datalium.Strata"
        echo "$dependency" | grep -qi 'analyzers' && fail "$id hides Datalium.Strata's source generator from its consumers"
    fi
done
unzip -Z1 "$packages/Datalium.Strata.$version.nupkg" | grep -qx 'analyzers/dotnet/cs/Datalium.Strata.SourceGenerators.dll' \
    || fail "Datalium.Strata doesn't contain the source generator"

dotnet run --project "$smoke" --configuration Release

echo "pack-smoke: five packages at $version, and an application built on them alone runs."
