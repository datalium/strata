//! What each SQL database's `strata_schema_version` table says
//! (`design/34-Storage-Hardening.md`, "Migrations"): one row per component
//! with the version its tables are at. Version 1 is the layout every store
//! brought an older database to before the table existed; later changes are
//! numbered steps. Both engines and the CLI read and write the same rows;
//! pinned by `schema/fixtures/checkpoint-log/schema-versions.json`. Mirrors
//! the .NET `StoreSchemaVersions`.

/// The table's name.
pub const TABLE: &str = "strata_schema_version";

/// The component of the checkpoint log, its head table and the leases.
pub const CHECKPOINTS: &str = "checkpoints";

/// The component of the history log.
pub const HISTORY: &str = "history";

/// The checkpoint component's version in a database this build has prepared:
/// 2 since value offload's `checkpoint_blobs` table
/// (`design/41-Checkpoint-Efficiency.md`, "Wire format").
pub const CHECKPOINTS_VERSION: i64 = 2;

/// The version a component's baseline brings a database to; later versions
/// are steps.
pub(crate) const BASELINE_VERSION: i64 = 1;

/// The history component's version in a database this build has prepared.
pub const HISTORY_VERSION: i64 = 1;

/// The component of the long-term memory tables
/// (`design/45-Long-Term-Memory.md`, "Schema version and migration").
pub const MEMORY: &str = "memory";

/// The memory component's version in a database this build has prepared.
pub const MEMORY_VERSION: i64 = 1;

/// The table in SQLite. Identical to `StoreSchemaVersions.SqliteDdl`; pinned
/// by `schema/fixtures/checkpoint-log/schema-versions-columns.json`.
pub const SCHEMA_VERSION_DDL: &str = "CREATE TABLE IF NOT EXISTS strata_schema_version (
    component TEXT NOT NULL PRIMARY KEY,
    version   INTEGER NOT NULL
)";

/// The table in Postgres. Identical to `StoreSchemaVersions.PostgresDdl`;
/// pinned by
/// `schema/fixtures/checkpoint-log/postgres-schema-versions-columns.json`.
pub const POSTGRES_SCHEMA_VERSION_DDL: &str = "CREATE TABLE IF NOT EXISTS strata_schema_version (
    component TEXT NOT NULL PRIMARY KEY,
    version   BIGINT NOT NULL
)";

/// Why a database at `found` can't be used by a build that knows `known`.
pub fn newer(component: &str, found: i64, known: i64) -> String {
    format!(
        "database is at {component} schema version {found}, and this build knows {known}: upgrade the runtime"
    )
}

/// Why a store that doesn't migrate refuses a database at `found`.
pub fn behind(component: &str, found: i64, needed: i64) -> String {
    format!(
        "database is at {component} schema version {found}, and this build needs {needed}: run its migration"
    )
}

/// A numbered change to a component's tables after version 1: the statements
/// that take a database from `version − 1` to `version`. Each must be safe to
/// run again: a step whose version wasn't recorded is run again. This build's
/// is version 2, value offload's `checkpoint_blobs`; a test adds its own
/// after it, as a later build would.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaStep {
    pub version: i64,
    pub name: String,
    pub statements: Vec<String>,
}

impl SchemaStep {
    pub fn new(
        version: i64,
        name: impl Into<String>,
        statements: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            version,
            name: name.into(),
            statements: statements.into_iter().map(Into::into).collect(),
        }
    }
}

/// The checkpoint component's steps after the baseline in SQLite, then
/// `later`.
pub(crate) fn sqlite_checkpoint_steps(later: &[SchemaStep]) -> Vec<SchemaStep> {
    let mut steps = vec![SchemaStep::new(
        2,
        "checkpoint_blobs",
        [crate::checkpoint::CHECKPOINT_BLOBS_DDL],
    )];
    steps.extend_from_slice(later);
    steps
}

/// The checkpoint component's steps after the baseline in Postgres, then
/// `later`.
pub(crate) fn postgres_checkpoint_steps(later: &[SchemaStep]) -> Vec<SchemaStep> {
    let mut steps = vec![SchemaStep::new(
        2,
        "checkpoint_blobs",
        [crate::postgres_store::POSTGRES_CHECKPOINT_BLOBS_DDL],
    )];
    steps.extend_from_slice(later);
    steps
}

/// The memory component's steps after its baseline in Postgres (none yet),
/// then `later`.
pub(crate) fn postgres_memory_steps(later: &[SchemaStep]) -> Vec<SchemaStep> {
    later.to_vec()
}

/// The memory component's steps after its baseline in SQLite (none yet), then
/// `later`.
pub(crate) fn sqlite_memory_steps(later: &[SchemaStep]) -> Vec<SchemaStep> {
    later.to_vec()
}

/// The version a build with `steps` after its baseline knows.
pub(crate) fn known(baseline: i64, steps: &[SchemaStep]) -> i64 {
    steps
        .iter()
        .map(|step| step.version)
        .max()
        .unwrap_or(baseline)
}

/// The steps of `steps` a database at `found` hasn't had, in order.
pub(crate) fn pending(steps: &[SchemaStep], found: i64) -> Vec<&SchemaStep> {
    let mut later: Vec<&SchemaStep> = steps.iter().filter(|step| step.version > found).collect();
    later.sort_by_key(|step| step.version);
    later
}
