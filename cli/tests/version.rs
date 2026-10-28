//! One version for the CLI, the crates and the .NET packages
//! (`design/36-Packaging-And-Versioning.md`): `strata --version` reports it,
//! and the three manifests that state it agree. `.github/scripts/versions.sh`
//! makes the same comparison for the release workflows; this one runs
//! wherever `cargo test` does.

use std::path::PathBuf;
use std::process::Command;

fn repository_file(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The `version` of `section` in a Cargo manifest.
fn cargo_version(manifest: &str, section: &str) -> String {
    let mut inside = false;
    for line in manifest.lines() {
        if line.starts_with('[') {
            inside = line.trim() == format!("[{section}]");
        } else if inside && line.trim_start().starts_with("version") {
            return line
                .split('"')
                .nth(1)
                .expect("a quoted version")
                .to_string();
        }
    }
    panic!("no version in [{section}]");
}

#[test]
fn the_cli_the_crates_and_the_dotnet_packages_share_one_version() {
    let cli = env!("CARGO_PKG_VERSION");
    let crates = cargo_version(&repository_file("rust/Cargo.toml"), "workspace.package");
    let props = repository_file("dotnet/Directory.Build.props");
    let dotnet = props
        .split("<Version>")
        .nth(1)
        .and_then(|rest| rest.split("</Version>").next())
        .expect("a <Version> in dotnet/Directory.Build.props");

    assert_eq!(crates, cli, "rust/Cargo.toml against cli/Cargo.toml");
    assert_eq!(
        dotnet, cli,
        "dotnet/Directory.Build.props against cli/Cargo.toml"
    );

    // The crates depend on their siblings at exactly that version.
    let workspace = repository_file("rust/Cargo.toml");
    let siblings: Vec<&str> = workspace
        .lines()
        .filter(|line| line.starts_with("datalium-strata") && line.contains("path ="))
        .collect();
    assert_eq!(siblings.len(), 6, "the six crates of the workspace");
    for sibling in siblings {
        assert!(
            sibling.contains(&format!("version = \"={cli}\"")),
            "{sibling}"
        );
    }
}

#[test]
fn strata_reports_its_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_strata"))
        .arg("--version")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("strata {}", env!("CARGO_PKG_VERSION"))
    );
}
