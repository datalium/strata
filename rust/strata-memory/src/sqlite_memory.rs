//! The SQLite long-term memory store (`design/45-Long-Term-Memory.md`, "SQLite
//! and sqlite-vec"): one ordinary table of records, searched exactly with
//! sqlite-vec's scalar `vec_distance_cosine`, every filter a plain `WHERE`
//! clause applied before ranking.
//!
//! **sqlite-vec, compiled in.** The `sqlite-vec` crate compiles sqlite-vec's C
//! source into this crate with `SQLITE_CORE`, so it calls the SQLite that
//! `rusqlite`'s `bundled` feature links. The store registers it on its own
//! connection when it opens (`sqlite3_vec_init` on the connection's handle),
//! not process-wide with `sqlite3_auto_extension`, so no other connection of
//! the process is changed. Only `vec_distance_cosine` is used: not the `vec0`
//! virtual table, whose metadata filtering is narrower. A connection on which
//! it can't be registered is refused:
//! `memory on SQLite needs the sqlite-vec extension: …`.
//!
//! **The layout doesn't depend on it.** A vector is a BLOB of little-endian
//! float32 values (sqlite-vec's own format), `content` and `metadata` are JSON
//! text (metadata a compact object, keys in byte order, `{}` when empty), and
//! timestamps are RFC 3339 text at whole seconds, as checkpoints' are, so a
//! purge compares them as text. SQLite's default collation is byte order, so
//! ties and listings go by id in byte order. Pinned by
//! `schema/fixtures/memory/sqlite-columns.json`.
//!
//! **Dimensions are fixed per table**, recorded in `memory_settings` (one row
//! per memory table; `schema/fixtures/memory/sqlite-settings-columns.json`):
//! the first open records them, and a later open with other dimensions is
//! refused. Two embedding sizes are two tables in one file
//! ([`SqliteMemoryOptions::with_table`]).
//!
//! **Shared with the SQLite checkpoint store:** the `strata_schema_version`
//! table, where the `memory` component is recorded at version 1
//! (`design/34-Storage-Hardening.md`, "Migrations"). A store always migrates
//! when it opens, in one immediate transaction, so a file shared with a
//! checkpoint store, or opened by two processes at once, works; a file a newer
//! build moved on is refused unchanged.
//!
//! **Threading,** as the SQLite checkpoint store's: one connection behind a
//! mutex, every call made on tokio's blocking pool. No retries: a local file
//! has no transient failures to wait out. Mirrors the .NET `SqliteMemoryStore`;
//! both create the same tables.

use std::collections::BTreeMap;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::path::Path;
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;
use datalium_strata::memory::{
    MAX_DIMENSIONS, MemoryKey, MemoryMatch, MemoryPage, MemoryPurge, MemoryQuery, MemoryRecord,
    MemoryRetention, MemorySearch, MemoryStore, MemoryStoreError,
};
use datalium_strata::store::timestamp_text;
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use crate::memory::{
    StoredRecord, content_text, metadata_text, observed, record_search_results, vector_bytes,
};
use crate::schema_versions;
use crate::support::{Clock, blocking, system_clock};

/// The table a store uses unless [`SqliteMemoryOptions::with_table`] names
/// another.
pub const DEFAULT_TABLE: &str = "memory_records";

/// The table that records each memory table's dimensions. Identical to the
/// .NET `SqliteMemoryStore`'s; pinned by
/// `schema/fixtures/memory/sqlite-settings-columns.json`.
pub const MEMORY_SETTINGS_DDL: &str = "CREATE TABLE IF NOT EXISTS memory_settings (
    table_name TEXT NOT NULL PRIMARY KEY,
    dimensions INTEGER NOT NULL
)";

/// The store's name on its metrics.
const STORE: &str = "sqlite";

/// Options of a [`SqliteMemoryStore`]. Mirrors the .NET
/// `SqliteMemoryOptions`.
#[derive(Clone)]
pub struct SqliteMemoryOptions {
    dimensions: usize,
    table: String,
    clock: Clock,
}

impl std::fmt::Debug for SqliteMemoryOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteMemoryOptions")
            .field("dimensions", &self.dimensions)
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl SqliteMemoryOptions {
    /// A store of vectors of `dimensions` (1 to [`MAX_DIMENSIONS`]), on
    /// [`DEFAULT_TABLE`].
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            table: DEFAULT_TABLE.to_string(),
            clock: system_clock(),
        }
    }

    /// The table: lowercase letters, digits and underscores, not starting
    /// with a digit, at most 63 characters (SQLite has no schemas). Its
    /// indexes are named after it.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    /// Stamps records on `clock` instead of the system clock: how a test makes
    /// time pass.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    /// Refuses options no store can open with, before the file is touched.
    fn check(&self) -> Result<(), MemoryStoreError> {
        if !(1..=MAX_DIMENSIONS).contains(&self.dimensions) {
            return Err(MemoryStoreError(format!(
                "a memory store has 1 to {MAX_DIMENSIONS} dimensions, not {}",
                self.dimensions
            )));
        }
        let table = &self.table;
        let identifier = !table.is_empty()
            && table.len() <= 63
            && !table.starts_with(|c: char| c.is_ascii_digit())
            && table
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !identifier {
            return Err(MemoryStoreError(format!(
                "the memory table '{table}' isn't a lowercase SQL name"
            )));
        }
        Ok(())
    }
}

/// The statements of one table, made once.
struct Sql {
    table: String,
    ddl: String,
    upsert: String,
    get: String,
    delete: String,
    purge: String,
}

/// What every statement that returns records selects, in this order.
const COLUMNS: &str = "t.namespace, t.tenant, t.id, t.model, t.embedding, t.content, t.text, \
     t.metadata, t.created_at, t.updated_at";

impl Sql {
    fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            ddl: format!(
                "CREATE TABLE IF NOT EXISTS {table} (
    namespace   TEXT NOT NULL,
    tenant      TEXT NOT NULL,
    id          TEXT NOT NULL,
    model       TEXT NOT NULL,
    embedding   BLOB NOT NULL,
    content     TEXT,
    text        TEXT,
    metadata    TEXT NOT NULL DEFAULT '{{}}',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (namespace, tenant, id)
);
CREATE INDEX IF NOT EXISTS {table}_scope ON {table} (namespace, tenant, model);
CREATE INDEX IF NOT EXISTS {table}_updated ON {table} (updated_at);"
            ),
            upsert: format!(
                "INSERT INTO {table} (namespace, tenant, id, model, embedding, content, text, metadata, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
                 ON CONFLICT (namespace, tenant, id) DO UPDATE SET
                    model = excluded.model, embedding = excluded.embedding, content = excluded.content,
                    text = excluded.text, metadata = excluded.metadata, updated_at = excluded.updated_at"
            ),
            get: format!(
                "SELECT {COLUMNS} FROM {table} t WHERE t.namespace = ?1 AND t.tenant = ?2 AND t.id = ?3"
            ),
            delete: format!(
                "DELETE FROM {table} WHERE namespace = ?1 AND tenant = ?2 AND id = ?3"
            ),
            purge: format!(
                "DELETE FROM {table} WHERE rowid IN (
                    SELECT rowid FROM {table}
                    WHERE updated_at < ?1
                      AND (?2 IS NULL OR namespace = ?2)
                      AND (?3 IS NULL OR tenant = ?3)
                    ORDER BY updated_at, namespace, tenant, id
                    LIMIT ?4)"
            ),
        }
    }

    /// A search with `pairs` metadata pairs: `?1` the query vector, `?2`
    /// namespace, `?3` tenant, `?4` model, `?5` the ids to exclude as a JSON
    /// array, then each pair's key and value, then the limit. Metadata is
    /// matched through `json_each`, not a JSON path, so a key may hold any
    /// character.
    fn search(&self, pairs: usize) -> String {
        let table = &self.table;
        let mut sql = format!(
            "SELECT {COLUMNS}, vec_distance_cosine(t.embedding, ?1) AS distance
             FROM {table} t
             WHERE t.namespace = ?2 AND t.tenant = ?3 AND t.model = ?4
               AND t.id NOT IN (SELECT value FROM json_each(?5))"
        );
        sql.push_str(&metadata_clauses(6, pairs));
        sql.push_str(&format!(
            "
             ORDER BY distance, t.id
             LIMIT ?{}",
            6 + 2 * pairs
        ));
        sql
    }

    /// A listing page with `pairs` metadata pairs: `?1` namespace, `?2`
    /// tenant, `?3` the id it continues after (or NULL), then each pair's key
    /// and value, then the limit.
    fn list(&self, pairs: usize) -> String {
        let table = &self.table;
        let mut sql = format!(
            "SELECT {COLUMNS} FROM {table} t
             WHERE t.namespace = ?1 AND t.tenant = ?2 AND (?3 IS NULL OR t.id > ?3)"
        );
        sql.push_str(&metadata_clauses(4, pairs));
        sql.push_str(&format!(
            "
             ORDER BY t.id
             LIMIT ?{}",
            4 + 2 * pairs
        ));
        sql
    }
}

/// One `EXISTS` per metadata pair, its key at `?first` and value after it.
fn metadata_clauses(first: usize, pairs: usize) -> String {
    (0..pairs)
        .map(|pair| {
            let key = first + 2 * pair;
            format!(
                "
               AND EXISTS (SELECT 1 FROM json_each(t.metadata) WHERE key = ?{key} AND value = ?{})",
                key + 1
            )
        })
        .collect()
}

/// The SQLite [`MemoryStore`] and [`MemoryRetention`]
/// (`design/45-Long-Term-Memory.md`). See the module's documentation.
pub struct SqliteMemoryStore {
    conn: Arc<Mutex<Connection>>,
    dimensions: usize,
    clock: Clock,
    sql: Arc<Sql>,
}

impl SqliteMemoryStore {
    /// Opens (or creates) the file at `path`, registers sqlite-vec on the
    /// connection, and creates or checks the store's tables. Refuses a table
    /// made with other dimensions and a file a newer build has moved on.
    pub fn open(
        path: impl AsRef<Path>,
        options: SqliteMemoryOptions,
    ) -> Result<Self, MemoryStoreError> {
        options.check()?;
        let conn = Connection::open(path).map_err(sqlite_err)?;
        Self::from_connection(conn, options)
    }

    /// [`open`](Self::open) on a database of its own in memory, gone when the
    /// store is dropped: for tests and development.
    pub fn open_in_memory(options: SqliteMemoryOptions) -> Result<Self, MemoryStoreError> {
        options.check()?;
        let conn = Connection::open_in_memory().map_err(sqlite_err)?;
        Self::from_connection(conn, options)
    }

    fn from_connection(
        mut conn: Connection,
        options: SqliteMemoryOptions,
    ) -> Result<Self, MemoryStoreError> {
        register_sqlite_vec(&conn)?;
        let sql = Sql::new(&options.table);
        migrate(&mut conn, &sql, options.dimensions)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            dimensions: options.dimensions,
            clock: options.clock,
            sql: Arc::new(sql),
        })
    }

    /// Runs `work` against the connection on the blocking pool.
    fn with_conn<R: Send + 'static>(
        &self,
        work: impl FnOnce(&mut Connection, &Sql) -> Result<R, MemoryStoreError> + Send + 'static,
    ) -> impl Future<Output = Result<R, MemoryStoreError>> + Send + 'static {
        let (conn, sql) = (self.conn.clone(), self.sql.clone());
        blocking(
            move || {
                let mut conn = conn
                    .lock()
                    .map_err(|_| MemoryStoreError("memory store lock poisoned".to_string()))?;
                work(&mut conn, &sql)
            },
            MemoryStoreError,
        )
    }

    async fn upsert_rows(&self, records: Vec<MemoryRecord>) -> Result<(), MemoryStoreError> {
        for record in &records {
            record.validate(self.dimensions)?;
        }
        if records.is_empty() {
            return Ok(());
        }
        let now = timestamp_text((self.clock)());
        let rows: Vec<[SqlValue; 9]> = records
            .iter()
            .map(|record| upsert_row(record, &now))
            .collect();
        observed(
            STORE,
            "upsert",
            self.with_conn(move |conn, sql| {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sqlite_err)?;
                {
                    let mut statement = tx.prepare_cached(&sql.upsert).map_err(sqlite_err)?;
                    for row in &rows {
                        statement
                            .execute(rusqlite::params_from_iter(row.iter()))
                            .map_err(sqlite_err)?;
                    }
                }
                tx.commit().map_err(sqlite_err)
            }),
        )
        .await
    }

    async fn get_row(&self, key: &MemoryKey) -> Result<Option<MemoryRecord>, MemoryStoreError> {
        key.validate()?;
        let key = key.clone();
        observed(
            STORE,
            "get",
            self.with_conn(move |conn, sql| {
                let mut statement = conn.prepare_cached(&sql.get).map_err(sqlite_err)?;
                statement
                    .query_row([&key.namespace, &key.tenant, &key.id], read_stored)
                    .optional()
                    .map_err(sqlite_err)?
                    .map(StoredRecord::into_record)
                    .transpose()
            }),
        )
        .await
    }

    async fn delete_row(&self, key: &MemoryKey) -> Result<bool, MemoryStoreError> {
        key.validate()?;
        let key = key.clone();
        observed(
            STORE,
            "delete",
            self.with_conn(move |conn, sql| {
                let mut statement = conn.prepare_cached(&sql.delete).map_err(sqlite_err)?;
                let deleted = statement
                    .execute([&key.namespace, &key.tenant, &key.id])
                    .map_err(sqlite_err)?;
                Ok(deleted > 0)
            }),
        )
        .await
    }

    async fn search_rows(
        &self,
        search: &MemorySearch,
    ) -> Result<Vec<MemoryMatch>, MemoryStoreError> {
        search.validate(self.dimensions)?;
        let mut params = vec![
            SqlValue::Blob(vector_bytes(&search.embedding.vector)),
            SqlValue::Text(search.namespace.clone()),
            SqlValue::Text(search.tenant.clone()),
            SqlValue::Text(search.embedding.model.clone()),
            SqlValue::Text(serde_json::to_string(&search.exclude).unwrap_or_else(|_| "[]".into())),
        ];
        push_pairs(&mut params, &search.metadata);
        params.push(SqlValue::Integer(search.limit as i64));
        let (pairs, min_similarity) = (search.metadata.len(), search.min_similarity);
        let matches = observed(
            STORE,
            "search",
            self.with_conn(move |conn, sql| {
                let mut statement = conn
                    .prepare_cached(&sql.search(pairs))
                    .map_err(sqlite_err)?;
                let mut rows = statement
                    .query(rusqlite::params_from_iter(params.iter()))
                    .map_err(sqlite_err)?;
                let mut matches = Vec::new();
                while let Some(row) = rows.next().map_err(sqlite_err)? {
                    let distance: f64 = row.get(10).map_err(sqlite_err)?;
                    let similarity = 1.0 - distance;
                    // Rows come most similar first: the first below the
                    // minimum ends the result.
                    if min_similarity.is_some_and(|min| similarity < min) {
                        break;
                    }
                    matches.push(MemoryMatch {
                        record: read_stored(row).map_err(sqlite_err)?.into_record()?,
                        similarity,
                    });
                }
                Ok(matches)
            }),
        )
        .await?;
        record_search_results(STORE, matches.len());
        Ok(matches)
    }

    async fn list_rows(&self, query: &MemoryQuery) -> Result<MemoryPage, MemoryStoreError> {
        let after = query.validate()?;
        let mut params = vec![
            SqlValue::Text(query.namespace.clone()),
            SqlValue::Text(query.tenant.clone()),
            after.map_or(SqlValue::Null, SqlValue::Text),
        ];
        push_pairs(&mut params, &query.metadata);
        // One more than the page, to tell whether another follows.
        params.push(SqlValue::Integer(query.limit as i64 + 1));
        let pairs = query.metadata.len();
        let records = observed(
            STORE,
            "list",
            self.with_conn(move |conn, sql| {
                let mut statement = conn.prepare_cached(&sql.list(pairs)).map_err(sqlite_err)?;
                let stored = statement
                    .query_map(rusqlite::params_from_iter(params.iter()), read_stored)
                    .map_err(sqlite_err)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sqlite_err)?;
                stored.into_iter().map(StoredRecord::into_record).collect()
            }),
        )
        .await?;
        Ok(MemoryPage::of(records, query.limit))
    }

    async fn purge_rows(&self, purge: &MemoryPurge) -> Result<usize, MemoryStoreError> {
        purge.validate()?;
        let params = [
            SqlValue::Text(purge.older_than_text()),
            purge
                .namespace
                .clone()
                .map_or(SqlValue::Null, SqlValue::Text),
            purge.tenant.clone().map_or(SqlValue::Null, SqlValue::Text),
            SqlValue::Integer(i64::try_from(purge.limit).unwrap_or(i64::MAX)),
        ];
        observed(
            STORE,
            "purge",
            self.with_conn(move |conn, sql| {
                let mut statement = conn.prepare_cached(&sql.purge).map_err(sqlite_err)?;
                statement
                    .execute(rusqlite::params_from_iter(params.iter()))
                    .map_err(sqlite_err)
            }),
        )
        .await
    }
}

/// sqlite-vec's entry point as SQLite calls an extension's: the `sqlite-vec`
/// crate declares it without its arguments.
type VecInit =
    unsafe extern "C" fn(*mut rusqlite::ffi::sqlite3, *mut *mut c_char, *const c_void) -> c_int;

/// Registers sqlite-vec's functions on `conn` alone, and checks that they
/// answer.
fn register_sqlite_vec(conn: &Connection) -> Result<(), MemoryStoreError> {
    let missing = |reason: String| {
        MemoryStoreError(format!(
            "memory on SQLite needs the sqlite-vec extension: {reason}"
        ))
    };
    // SAFETY: `sqlite3_vec_init` is sqlite-vec's extension entry point,
    // `int sqlite3_vec_init(sqlite3*, char**, const sqlite3_api_routines*)`;
    // compiled with `SQLITE_CORE`, it ignores the routines and calls the
    // linked SQLite directly. The handle is the open connection's, used on
    // this thread before the connection is shared.
    let (rc, message) = unsafe {
        let init: VecInit = std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ());
        let mut message: *mut c_char = std::ptr::null_mut();
        let rc = init(conn.handle(), &mut message, std::ptr::null());
        let text = if message.is_null() {
            None
        } else {
            let text = CStr::from_ptr(message).to_string_lossy().into_owned();
            rusqlite::ffi::sqlite3_free(message.cast());
            Some(text)
        };
        (rc, text)
    };
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(missing(message.unwrap_or_else(|| {
            format!("sqlite3_vec_init failed with code {rc}")
        })));
    }
    conn.query_row("SELECT vec_version()", [], |row| row.get::<_, String>(0))
        .map_err(|err| missing(err.to_string()))?;
    Ok(())
}

/// Creates or checks the store's tables in one immediate transaction, so
/// concurrent first opens can't interleave (`design/34-Storage-Hardening.md`,
/// "Migrations"): reads the `memory` component's version (no row: 0), refuses
/// a file newer than this build, creates the tables and indexes when missing
/// (version 1; a second table in a file already at 1 is created too), runs
/// any later step, records the version, and records or checks the table's
/// dimensions. A refusal rolls everything back.
fn migrate(conn: &mut Connection, sql: &Sql, dimensions: usize) -> Result<(), MemoryStoreError> {
    let steps = schema_versions::sqlite_memory_steps(&[]);
    let known = schema_versions::known(schema_versions::MEMORY_VERSION, &steps);
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_err)?;
    tx.execute_batch(schema_versions::SCHEMA_VERSION_DDL)
        .map_err(sqlite_err)?;
    let found: i64 = tx
        .query_row(
            "SELECT version FROM strata_schema_version WHERE component = ?1",
            [schema_versions::MEMORY],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_err)?
        .unwrap_or(0);
    if found > known {
        return Err(MemoryStoreError(schema_versions::newer(
            schema_versions::MEMORY,
            found,
            known,
        )));
    }
    tx.execute_batch(MEMORY_SETTINGS_DDL).map_err(sqlite_err)?;
    tx.execute_batch(&sql.ddl).map_err(sqlite_err)?;
    if found < known {
        // Version 1 is the tables, made above or already there.
        for step in schema_versions::pending(&steps, found.max(schema_versions::MEMORY_VERSION)) {
            for statement in &step.statements {
                tx.execute_batch(statement).map_err(sqlite_err)?;
            }
        }
        tx.execute(
            "INSERT INTO strata_schema_version (component, version) VALUES (?1, ?2)
             ON CONFLICT (component) DO UPDATE SET version = excluded.version",
            rusqlite::params![schema_versions::MEMORY, known],
        )
        .map_err(sqlite_err)?;
    }
    tx.execute(
        "INSERT INTO memory_settings (table_name, dimensions) VALUES (?1, ?2)
         ON CONFLICT (table_name) DO NOTHING",
        rusqlite::params![sql.table, dimensions as i64],
    )
    .map_err(sqlite_err)?;
    let recorded: i64 = tx
        .query_row(
            "SELECT dimensions FROM memory_settings WHERE table_name = ?1",
            [&sql.table],
            |row| row.get(0),
        )
        .map_err(sqlite_err)?;
    if recorded != dimensions as i64 {
        return Err(MemoryStoreError(format!(
            "memory was created with {recorded} dimensions, and this store was opened with {dimensions}"
        )));
    }
    tx.commit().map_err(sqlite_err)
}

fn sqlite_err(err: rusqlite::Error) -> MemoryStoreError {
    MemoryStoreError(err.to_string())
}

/// Each metadata pair's key and value, as parameters.
fn push_pairs(params: &mut Vec<SqlValue>, pairs: &BTreeMap<String, String>) {
    for (key, value) in pairs {
        params.push(SqlValue::Text(key.clone()));
        params.push(SqlValue::Text(value.clone()));
    }
}

/// A record's upsert parameters, `?1` to `?9`.
fn upsert_row(record: &MemoryRecord, now: &str) -> [SqlValue; 9] {
    let optional = |text: Option<String>| text.map_or(SqlValue::Null, SqlValue::Text);
    [
        SqlValue::Text(record.namespace.clone()),
        SqlValue::Text(record.tenant.clone()),
        SqlValue::Text(record.id.clone()),
        SqlValue::Text(record.embedding.model.clone()),
        SqlValue::Blob(vector_bytes(&record.embedding.vector)),
        optional(content_text(&record.content)),
        optional(record.text.clone()),
        SqlValue::Text(metadata_text(&record.metadata)),
        SqlValue::Text(now.to_string()),
    ]
}

/// A row of [`COLUMNS`] as stored.
fn read_stored(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredRecord> {
    Ok(StoredRecord {
        namespace: row.get(0)?,
        tenant: row.get(1)?,
        id: row.get(2)?,
        model: row.get(3)?,
        embedding: row.get(4)?,
        content: row.get(5)?,
        text: row.get(6)?,
        metadata: row.get(7)?,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
    })
}

impl MemoryStore for SqliteMemoryStore {
    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn upsert(&self, record: MemoryRecord) -> BoxFuture<'_, Result<(), MemoryStoreError>> {
        Box::pin(self.upsert_rows(vec![record]))
    }

    fn upsert_many(
        &self,
        records: Vec<MemoryRecord>,
    ) -> BoxFuture<'_, Result<(), MemoryStoreError>> {
        Box::pin(self.upsert_rows(records))
    }

    fn get<'a>(
        &'a self,
        key: &'a MemoryKey,
    ) -> BoxFuture<'a, Result<Option<MemoryRecord>, MemoryStoreError>> {
        Box::pin(self.get_row(key))
    }

    fn delete<'a>(&'a self, key: &'a MemoryKey) -> BoxFuture<'a, Result<bool, MemoryStoreError>> {
        Box::pin(self.delete_row(key))
    }

    fn search<'a>(
        &'a self,
        search: &'a MemorySearch,
    ) -> BoxFuture<'a, Result<Vec<MemoryMatch>, MemoryStoreError>> {
        Box::pin(self.search_rows(search))
    }

    fn list<'a>(
        &'a self,
        query: &'a MemoryQuery,
    ) -> BoxFuture<'a, Result<MemoryPage, MemoryStoreError>> {
        Box::pin(self.list_rows(query))
    }
}

impl MemoryRetention for SqliteMemoryStore {
    fn purge<'a>(
        &'a self,
        purge: &'a MemoryPurge,
    ) -> BoxFuture<'a, Result<usize, MemoryStoreError>> {
        Box::pin(self.purge_rows(purge))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        register_sqlite_vec(&conn).unwrap();
        conn
    }

    #[test]
    fn sqlite_vec_is_registered_on_the_connection_and_computes_cosine_distance() {
        let conn = connection();
        let version: String = conn
            .query_row("SELECT vec_version()", [], |row| row.get(0))
            .unwrap();
        assert!(version.starts_with('v'), "{version}");
        let distance = |a: &[f32], b: &[f32]| -> f64 {
            conn.query_row(
                "SELECT vec_distance_cosine(?1, ?2)",
                [vector_bytes(a), vector_bytes(b)],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert!(distance(&[1.0, 0.0, 0.0], &[3.0, 0.0, 0.0]).abs() < 1e-7);
        assert!((distance(&[1.0, 0.0, 0.0], &[0.0, 1.0, 0.0]) - 1.0).abs() < 1e-7);
        assert!((distance(&[1.0, 0.0, 0.0], &[-1.0, 0.0, 0.0]) - 2.0).abs() < 1e-7);
        assert!(
            (distance(&[1.0, 0.0, 0.0], &[1.0, 1.0, 0.0])
                - (1.0 - std::f64::consts::FRAC_1_SQRT_2))
                .abs()
                < 1e-6
        );
    }

    #[test]
    fn another_connection_of_the_process_is_left_without_it() {
        let _registered = connection();
        let plain = Connection::open_in_memory().unwrap();
        assert!(
            plain
                .query_row("SELECT vec_version()", [], |row| row.get::<_, String>(0))
                .is_err()
        );
    }

    #[test]
    fn options_no_store_can_open_with_are_refused() {
        let refusal = |options: SqliteMemoryOptions| options.check().unwrap_err().0;
        assert_eq!(
            refusal(SqliteMemoryOptions::new(0)),
            "a memory store has 1 to 16000 dimensions, not 0"
        );
        assert_eq!(
            refusal(SqliteMemoryOptions::new(16_001)),
            "a memory store has 1 to 16000 dimensions, not 16001"
        );
        for table in [
            "Memory",
            "memory-records",
            "1memory",
            "main.memory",
            "",
            "memory;drop",
        ] {
            assert_eq!(
                refusal(SqliteMemoryOptions::new(3).with_table(table)),
                format!("the memory table '{table}' isn't a lowercase SQL name")
            );
        }
        assert_eq!(
            refusal(SqliteMemoryOptions::new(3).with_table("m".repeat(64))),
            format!(
                "the memory table '{}' isn't a lowercase SQL name",
                "m".repeat(64)
            )
        );
        assert!(
            SqliteMemoryOptions::new(3)
                .with_table("_facts_768")
                .check()
                .is_ok()
        );
        assert!(SqliteMemoryOptions::new(16_000).check().is_ok());
    }

    #[test]
    fn a_search_with_metadata_matches_each_pair_through_json_each() {
        let sql = Sql::new("facts").search(2);
        assert!(sql.contains("json_each(t.metadata) WHERE key = ?6 AND value = ?7"));
        assert!(sql.contains("json_each(t.metadata) WHERE key = ?8 AND value = ?9"));
        assert!(sql.trim_end().ends_with("LIMIT ?10"), "{sql}");
        assert!(Sql::new("facts").list(0).trim_end().ends_with("LIMIT ?4"));
    }
}
