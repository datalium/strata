//! Types for `schema/history-record.schema.json`.
//!
//! Must stay in lockstep with the JSON Schema file and with the C#
//! equivalent in `Datalium.Strata.Storage/Schema/HistoryRecord.cs`. See
//! `schema/README.md`.

use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryRecord {
    pub schema_version: String,
    pub session_id: String,
    pub sequence: u64,
    pub role: MessageRole,
    pub content: String,
    pub timestamp: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::path::PathBuf;

    fn fixture(name: &str) -> String {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e))
    }

    #[test]
    fn history_record_round_trips_through_the_shared_fixture() {
        let raw = fixture("history-record.sample.json");
        let original: Value = serde_json::from_str(&raw).unwrap();

        let record: HistoryRecord = serde_json::from_str(&raw).unwrap();
        let round_tripped = serde_json::to_value(&record).unwrap();

        assert_eq!(original, round_tripped);
    }
}
