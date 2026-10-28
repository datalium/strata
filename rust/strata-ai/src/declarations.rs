//! Tool names a provider accepts (`design/44-LLM-Integration.md`, "Tool
//! names"): a registry's tools as declarations under provider-safe names,
//! and the map back.

use std::collections::{HashMap, HashSet};

use datalium_strata::tools::ToolRegistry;
use serde_json::Value;

use crate::types::ToolDeclaration;

/// The longest name providers accept.
const MAX_NAME: usize = 64;

/// A registry's tools as a model sees them, and the way back. Mirrors .NET's
/// `ToolDeclarations`.
///
/// Providers accept tool names matching `^[a-zA-Z0-9_-]{1,64}$`, and Strata's
/// are free text (`k8s.deployment`). Each other character becomes `_`, the
/// name is cut to 64 characters, and a name already taken gets `_2`, `_3`, …
/// (cut to fit), in registration order: `k8s.deployment` is `k8s_deployment`.
/// Built once per agent, so the model sees the same names on every turn.
#[derive(Debug, Clone, Default)]
pub struct ToolDeclarations {
    declarations: Vec<ToolDeclaration>,
    strata: HashMap<String, String>,
}

impl ToolDeclarations {
    /// The declarations of every tool in `registry`, in its order.
    pub fn for_registry(registry: &ToolRegistry) -> Self {
        let Value::Array(manifest) = registry.manifest() else {
            return Self::default();
        };
        let mut taken = HashSet::new();
        let mut declarations = Vec::with_capacity(manifest.len());
        let mut strata = HashMap::with_capacity(manifest.len());
        for entry in manifest {
            let Some(name) = entry.get("name").and_then(Value::as_str) else {
                continue;
            };
            let safe = unique(provider_name(name), &mut taken);
            declarations.push(ToolDeclaration::new(
                safe.clone(),
                entry
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                entry.get("parameters").cloned().unwrap_or_else(
                    || serde_json::json!({ "type": "object", "properties": {}, "required": [] }),
                ),
            ));
            strata.insert(safe, name.to_string());
        }
        Self {
            declarations,
            strata,
        }
    }

    /// The declarations, under the names the model sees.
    pub fn declarations(&self) -> &[ToolDeclaration] {
        &self.declarations
    }

    /// The Strata name of the tool the model calls `name`, or `name` itself
    /// when no declaration has it: the registry then refuses it as not found.
    pub fn strata_name<'a>(&'a self, name: &'a str) -> &'a str {
        self.strata.get(name).map_or(name, String::as_str)
    }

    /// The name the model sees for the Strata tool `name`.
    pub fn provider_name_of(&self, name: &str) -> Option<&str> {
        self.strata
            .iter()
            .find(|(_, strata)| strata.as_str() == name)
            .map(|(safe, _)| safe.as_str())
    }
}

/// `name` with every character outside `[a-zA-Z0-9_-]` as `_`, cut to 64.
pub(crate) fn provider_name(name: &str) -> String {
    let safe: String = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .take(MAX_NAME)
        .collect();
    if safe.is_empty() {
        "_".to_string()
    } else {
        safe
    }
}

/// `name`, or the first of `name_2`, `name_3`, … (cut to fit 64) not taken.
fn unique(name: String, taken: &mut HashSet<String>) -> String {
    if taken.insert(name.clone()) {
        return name;
    }
    for n in 2.. {
        let suffix = format!("_{n}");
        let candidate = format!(
            "{}{suffix}",
            &name[..name.len().min(MAX_NAME - suffix.len())]
        );
        if taken.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("some suffix is free")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_made_safe_cut_and_unique() {
        assert_eq!(provider_name("k8s.deployment"), "k8s_deployment");
        assert_eq!(provider_name("météo/now"), "m_t_o_now");
        assert_eq!(provider_name(""), "_");
        let long = "a".repeat(70);
        assert_eq!(provider_name(&long).len(), 64);

        let mut taken = HashSet::new();
        assert_eq!(unique("a_b".into(), &mut taken), "a_b");
        assert_eq!(unique("a_b".into(), &mut taken), "a_b_2");
        assert_eq!(unique("a_b".into(), &mut taken), "a_b_3");
        let cut = unique(provider_name(&long), &mut taken);
        assert_eq!(cut, "a".repeat(64));
        let second = unique(provider_name(&long), &mut taken);
        assert_eq!(second, format!("{}_2", "a".repeat(62)));
        assert_eq!(second.len(), 64);
    }
}
