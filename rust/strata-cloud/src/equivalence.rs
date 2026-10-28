//! How a checkpoint compares with Cloud's copy of it
//! (`design/30-Strata-Cloud-Store.md`, "Normalisation").

use chrono::DateTime;
use datalium_strata::schema::Checkpoint;
use serde_json::Value;

/// Whether `left` and `right` are the same checkpoint as Cloud sees it:
/// object keys in any order, numbers by value (`1.0` equals `1`), and
/// `created_at` / `resumed_at` as instants at whole seconds. Everything else
/// exactly. Cloud stores `frozen_state` and `metadata` as JSONB and normalises
/// timestamps, so its copy can differ in bytes but not in content.
pub fn equivalent(left: &Checkpoint, right: &Checkpoint) -> bool {
    match (serde_json::to_value(left), serde_json::to_value(right)) {
        (Ok(left), Ok(right)) => values_equivalent(&left, &right, None),
        _ => false,
    }
}

fn values_equivalent(left: &Value, right: &Value, key: Option<&str>) -> bool {
    match (left, right) {
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(name, value)| {
                    right
                        .get(name)
                        .is_some_and(|other| values_equivalent(value, other, Some(name)))
                })
        }
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| values_equivalent(left, right, None))
        }
        (Value::Number(left), Value::Number(right)) => {
            match (left.as_i64(), right.as_i64(), left.as_u64(), right.as_u64()) {
                (Some(left), Some(right), _, _) => left == right,
                (_, _, Some(left), Some(right)) => left == right,
                _ => left.as_f64() == right.as_f64(),
            }
        }
        (Value::String(left), Value::String(right)) => {
            left == right
                || (matches!(key, Some("created_at" | "resumed_at")) && same_second(left, right))
        }
        _ => left == right,
    }
}

fn same_second(left: &str, right: &str) -> bool {
    match (
        DateTime::parse_from_rfc3339(left),
        DateTime::parse_from_rfc3339(right),
    ) {
        (Ok(left), Ok(right)) => left.timestamp() == right.timestamp(),
        _ => false,
    }
}
