//! Runs a sample's `main` in a child process: for samples that are whole programs, which read their
//! arguments or print what they did. The child is this test binary again, running one test, so it gets
//! the arguments the sample reads, a working directory of its own, and its output can be checked.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

const CHILD: &str = "STRATA_DOCS_SNIPPETS_CHILD";
const BEGIN: &str = "--- the sample's output begins ---";
const END: &str = "--- the sample's output ends ---";

/// In the child that [`run`] starts, calls `main` between the markers [`run`] looks for. Anywhere else it
/// does nothing, so the test that holds it passes at once in an ordinary `cargo test`.
pub(crate) fn main_in_child(main: impl FnOnce()) {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    println!("{BEGIN}");
    main();
    println!();
    println!("{END}");
}

/// Runs the test `test` (its path in this crate, e.g. `start_here::quickstart::main_runs`) in a child
/// process, in `dir`, with `arg` as its first argument, and returns what the sample printed.
pub(crate) fn run(test: &str, arg: &str, dir: &Path) -> String {
    // `--exact` keeps `arg` from matching any test: it is there only for the sample to read.
    let output = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([arg, test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .current_dir(dir)
        .output()
        .expect("the test binary starts");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{test} failed in the child process:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let start = stdout
        .find(BEGIN)
        .unwrap_or_else(|| panic!("{test} didn't run in the child process:\n{stdout}"))
        + BEGIN.len();
    let end = stdout[start..].find(END).expect("the child's output ends") + start;
    stdout[start..end].trim().to_string()
}

/// An empty directory of its own under the system's temp directory, removed when dropped.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(label: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "strata-docs-snippets-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("the temp directory can be created");
        Self(path)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
