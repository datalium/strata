//! Tool names as `&'static str` (`design/50-MCP-Client.md`, "Tool names in
//! Rust"). `StrataTool::name()` returns `&'static str`, and 0.9.0's API
//! promise keeps it that way, but an MCP tool's name is known only once the
//! server has been listed. Each distinct name is leaked once, here, and
//! every later tool set with that name gets the same `&'static str`. So a
//! reconnect, a restarted server or a second client of the same server adds
//! nothing: what this holds is bounded by how many different names the
//! process ever sees, not by how often it connects.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock, PoisonError};

fn names() -> &'static Mutex<HashSet<&'static str>> {
    static NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    NAMES.get_or_init(Default::default)
}

/// The one `&'static str` for `name`, leaked the first time it's seen.
pub(crate) fn intern(name: &str) -> &'static str {
    // A panic while holding the lock can't leave the set half-changed: an
    // insert either happened or it didn't.
    let mut names = names().lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(&name) = names.get(name) {
        return name;
    }
    let name: &'static str = Box::leak(name.into());
    names.insert(name);
    name
}

#[cfg(test)]
pub(crate) fn interned() -> usize {
    names().lock().unwrap_or_else(PoisonError::into_inner).len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_seen_again_is_the_same_str_and_leaks_nothing_more() {
        let first = intern(&format!("names.test.{}", "echo"));
        let before = interned();
        for _ in 0..1000 {
            let again = intern(&format!("names.test.{}", "echo"));
            assert!(std::ptr::eq(first, again));
        }
        assert_eq!(interned(), before);
        assert_eq!(first, "names.test.echo");
        assert!(!std::ptr::eq(first, intern("names.test.other")));
    }
}
