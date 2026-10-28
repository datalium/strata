//! The Postgres long-term memory store (`design/45-Long-Term-Memory.md`,
//! "Postgres"): one table of records with a pgvector `vector(D)` column,
//! searched by exact cosine distance (or through an HNSW index), with the
//! filters applied before ranking.
//!
//! **pgvector, without its client type.** Vectors go in as text cast to
//! `vector` (`CAST($1::text AS vector)`, written `[1,0.5,-0.25]` with the
//! shortest digits that read back the same float) and come out as
//! `embedding::text`, so this crate needs no pgvector client. The database
//! needs the `vector` extension: the migration runs
//! `CREATE EXTENSION IF NOT EXISTS vector` and refuses without it.
//!
//! **Dimensions are fixed per table.** The first open creates the column as
//! `vector(D)`; a later open with other dimensions is refused. Two embedding
//! sizes are two stores, on two tables ([`PostgresMemoryOptions::with_table`]).
//!
//! **Shared with the checkpoint stores:** the connection pool
//! (`deadpool-postgres`), the [`StoreRetry`] every call runs under (a retry is
//! counted as `strata.store.operation` `memory`), and `strata_schema_version`,
//! where the `memory` component is recorded (`design/34-Storage-Hardening.md`,
//! "Migrations"). Every call is idempotent, so a call that failed for a
//! transient reason is made again whole: an upsert writes the same row, a
//! delete or a purge deletes what is still there.
//!
//! **Timestamps** are the store's clock (the process's, unless
//! [`PostgresMemoryOptions::with_clock`] gives another), stored as RFC 3339
//! text at whole seconds, as checkpoints' `created_at` is, so a purge compares
//! them as text. `namespace`, `tenant` and `id` are `COLLATE "C"`, and every
//! ordering names it, so ties and listings go by id in byte order whatever the
//! database's collation. Mirrors the .NET `PostgresMemoryStore`; both create
//! the same table (`schema/fixtures/memory/postgres-columns.json`).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::memory::{
    Embedding, MAX_DIMENSIONS, MemoryKey, MemoryMatch, MemoryPage, MemoryPurge, MemoryQuery,
    MemoryRecord, MemoryRetention, MemorySearch, MemoryStore, MemoryStoreError,
};
use datalium_strata::store::timestamp_text;
use deadpool_postgres::Pool;
use serde_json::Value;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row, Transaction};

use crate::memory::{MemoryIndex, observed, record_search_results};
use crate::postgres_store::{
    DEFAULT_POOL_SIZE, Link, PostgresStoreOptions, locked_version, run_steps,
};
use crate::retry::{Failure, StoreRetry};
use crate::schema_versions;
use crate::support::{Clock, system_clock};

/// The table a store uses unless [`PostgresMemoryOptions::with_table`] names
/// another.
pub const DEFAULT_TABLE: &str = "memory_records";

/// The most dimensions pgvector's HNSW index takes.
pub const MAX_HNSW_DIMENSIONS: usize = 2_000;

/// The store's name on its metrics.
const STORE: &str = "postgres";

/// What a retry of any of its calls is counted as.
const OPERATION: &str = "memory";

/// Options of a [`PostgresMemoryStore`]. Mirrors the .NET
/// `PostgresMemoryOptions`.
#[derive(Clone)]
pub struct PostgresMemoryOptions {
    dimensions: usize,
    table: String,
    index: MemoryIndex,
    retry: StoreRetry,
    auto_migrate: bool,
    pool_size: usize,
    clock: Clock,
}

impl std::fmt::Debug for PostgresMemoryOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresMemoryOptions")
            .field("dimensions", &self.dimensions)
            .field("table", &self.table)
            .field("index", &self.index)
            .field("retry", &self.retry)
            .field("auto_migrate", &self.auto_migrate)
            .field("pool_size", &self.pool_size)
            .finish_non_exhaustive()
    }
}

impl PostgresMemoryOptions {
    /// A store of vectors of `dimensions` (1 to [`MAX_DIMENSIONS`], pgvector's
    /// limit for a column), on
    /// [`DEFAULT_TABLE`], exact, migrating on open.
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            table: DEFAULT_TABLE.to_string(),
            index: MemoryIndex::None,
            retry: StoreRetry::new(),
            auto_migrate: true,
            pool_size: DEFAULT_POOL_SIZE,
            clock: system_clock(),
        }
    }

    /// The table, `name` or `schema.name`, each part lowercase letters,
    /// digits and underscores, not starting with a digit. Its indexes are
    /// named after it.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    /// How searches find the nearest records ([`MemoryIndex::None`], exact,
    /// by default). An HNSW index is created when the store migrates.
    pub fn with_index(mut self, index: MemoryIndex) -> Self {
        self.index = index;
        self
    }

    /// How a call that failed for a transient reason is repeated.
    /// [`StoreRetry::none`] turns it off.
    pub fn with_retry(mut self, retry: StoreRetry) -> Self {
        self.retry = retry;
        self
    }

    /// Whether the store creates its table and brings the `memory` component
    /// up to date when it is opened (the default). When `false` it only
    /// checks them, and refuses a database that is behind: a deploy step then
    /// calls [`PostgresMemoryStore::migrate`].
    pub fn with_auto_migrate(mut self, auto_migrate: bool) -> Self {
        self.auto_migrate = auto_migrate;
        self
    }

    /// How many connections a store built with
    /// [`connect`](PostgresMemoryStore::connect) keeps at most (16 by
    /// default). At least 1.
    pub fn with_pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
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

    pub fn index(&self) -> MemoryIndex {
        self.index
    }

    /// Refuses options no store can open with, before anything connects.
    fn check(&self) -> Result<(), MemoryStoreError> {
        if !(1..=MAX_DIMENSIONS).contains(&self.dimensions) {
            return Err(MemoryStoreError(format!(
                "a memory store has 1 to {MAX_DIMENSIONS} dimensions, not {}",
                self.dimensions
            )));
        }
        if matches!(self.index, MemoryIndex::Hnsw { .. }) && self.dimensions > MAX_HNSW_DIMENSIONS {
            return Err(MemoryStoreError(format!(
                "an HNSW index takes at most {MAX_HNSW_DIMENSIONS} dimensions, not {}",
                self.dimensions
            )));
        }
        let parts: Vec<&str> = self.table.split('.').collect();
        let identifier = |part: &&str| {
            !part.is_empty()
                && part.len() <= 63
                && !part.starts_with(|c: char| c.is_ascii_digit())
                && part
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        };
        if parts.len() > 2 || !parts.iter().all(identifier) {
            return Err(MemoryStoreError(format!(
                "the memory table '{}' isn't a lowercase SQL name (name or schema.name)",
                self.table
            )));
        }
        Ok(())
    }

    /// The connection options the checkpoint stores' link takes.
    fn link_options(&self) -> PostgresStoreOptions {
        PostgresStoreOptions::new()
            .with_retry(self.retry.clone())
            .with_pool_size(self.pool_size)
    }
}

/// The statements of one table, made once.
struct Sql {
    table: String,
    ddl: String,
    hnsw: String,
    upsert: String,
    get: String,
    delete: String,
    search: String,
    search_hnsw: String,
    list: String,
    purge: String,
}

/// What every statement that returns records selects.
const COLUMNS: &str = "namespace, tenant, id, model, embedding::text AS embedding_text, content, text, metadata, created_at, updated_at";

/// The filters of a search: `$1` namespace, `$2` tenant, `$3` model, `$5` ids
/// to exclude, `$6` metadata pairs. `$4` is the query vector and `$7` the
/// limit.
const SEARCH_WHERE: &str = "namespace = $1 AND tenant = $2 AND model = $3 \
     AND NOT (id = ANY($5)) AND metadata @> $6";

impl Sql {
    fn new(table: &str, dimensions: usize, index: MemoryIndex) -> Self {
        // An index is named after the table, in the table's schema.
        let name = table.rsplit('.').next().unwrap_or(table);
        let hnsw = match index {
            MemoryIndex::Hnsw { m, ef_construction } => format!(
                "CREATE INDEX IF NOT EXISTS {name}_embedding_hnsw ON {table} \
                 USING hnsw (embedding vector_cosine_ops) WITH (m = {m}, ef_construction = {ef_construction})"
            ),
            MemoryIndex::None => String::new(),
        };
        Self {
            table: table.to_string(),
            ddl: format!(
                r#"CREATE TABLE IF NOT EXISTS {table} (
    namespace   TEXT COLLATE "C" NOT NULL,
    tenant      TEXT COLLATE "C" NOT NULL,
    id          TEXT COLLATE "C" NOT NULL,
    model       TEXT NOT NULL,
    embedding   vector({dimensions}) NOT NULL,
    content     JSONB,
    text        TEXT,
    metadata    JSONB NOT NULL DEFAULT '{{}}',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (namespace, tenant, id)
);
CREATE INDEX IF NOT EXISTS {name}_scope ON {table} (namespace, tenant, model);
CREATE INDEX IF NOT EXISTS {name}_updated ON {table} (updated_at);"#
            ),
            hnsw,
            upsert: format!(
                "INSERT INTO {table} (namespace, tenant, id, model, embedding, content, text, metadata, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, CAST($5::text AS vector), $6, $7, $8, $9, $9)
                 ON CONFLICT (namespace, tenant, id) DO UPDATE SET
                    model = EXCLUDED.model, embedding = EXCLUDED.embedding, content = EXCLUDED.content,
                    text = EXCLUDED.text, metadata = EXCLUDED.metadata, updated_at = EXCLUDED.updated_at"
            ),
            get: format!(
                "SELECT {COLUMNS} FROM {table} WHERE namespace = $1 AND tenant = $2 AND id = $3"
            ),
            delete: format!("DELETE FROM {table} WHERE namespace = $1 AND tenant = $2 AND id = $3"),
            search: format!(
                r#"SELECT {COLUMNS}, 1 - (embedding <=> CAST($4::text AS vector)) AS similarity
                 FROM {table} WHERE {SEARCH_WHERE}
                 ORDER BY embedding <=> CAST($4::text AS vector), id COLLATE "C"
                 LIMIT $7"#
            ),
            // The index orders by distance alone, and an iterative scan
            // (`relaxed_order`) may return rows slightly out of order: twice
            // the limit's nearest (`$8`) are taken through the index, then
            // sorted again by exact distance and id and cut at the limit
            // (pgvector's own advice for iterative scans). The .NET store's
            // query is the same.
            search_hnsw: format!(
                r#"WITH nearest AS MATERIALIZED (
                    SELECT {COLUMNS}, embedding <=> CAST($4::text AS vector) AS distance
                    FROM {table} WHERE {SEARCH_WHERE}
                    ORDER BY embedding <=> CAST($4::text AS vector)
                    LIMIT $8)
                 SELECT namespace, tenant, id, model, embedding_text, content, text, metadata,
                        created_at, updated_at, 1 - distance AS similarity
                 FROM nearest ORDER BY distance, id COLLATE "C"
                 LIMIT $7"#
            ),
            list: format!(
                r#"SELECT {COLUMNS} FROM {table}
                 WHERE namespace = $1 AND tenant = $2 AND metadata @> $3
                   AND ($4::text IS NULL OR id COLLATE "C" > $4::text)
                 ORDER BY id COLLATE "C" LIMIT $5"#
            ),
            // The outer `updated_at < $1` is checked again on a row another
            // session changed while this one waited for it, so a record
            // written meanwhile is kept.
            purge: format!(
                r#"DELETE FROM {table} WHERE updated_at < $1 AND (namespace, tenant, id) IN (
                    SELECT namespace, tenant, id FROM {table}
                    WHERE updated_at < $1
                      AND ($2::text IS NULL OR namespace = $2::text)
                      AND ($3::text IS NULL OR tenant = $3::text)
                    ORDER BY updated_at COLLATE "C", namespace COLLATE "C", tenant COLLATE "C", id COLLATE "C"
                    LIMIT $4)"#
            ),
        }
    }
}

/// The Postgres [`MemoryStore`] and [`MemoryRetention`]
/// (`design/45-Long-Term-Memory.md`). See the module's documentation.
pub struct PostgresMemoryStore {
    link: Link,
    dimensions: usize,
    index: MemoryIndex,
    clock: Clock,
    sql: Sql,
}

impl PostgresMemoryStore {
    /// Connects to `url` with a pool of its own, and creates or checks its
    /// table (see [`PostgresMemoryOptions::with_auto_migrate`]). Refuses a
    /// table made with other dimensions, and a database without pgvector.
    pub async fn connect(
        url: &str,
        options: PostgresMemoryOptions,
    ) -> Result<Self, MemoryStoreError> {
        options.check()?;
        let link = Link::connect_for(url, &options.link_options(), OPERATION).await?;
        Self::prepared(link, options).await
    }

    /// Over a pool the application keeps, which it may share with its other
    /// stores; [`PostgresMemoryOptions::with_pool_size`] doesn't apply.
    pub async fn from_pool(
        pool: Pool,
        options: PostgresMemoryOptions,
    ) -> Result<Self, MemoryStoreError> {
        options.check()?;
        let link = Link::pooled(pool, &options.link_options());
        Self::prepared(link, options).await
    }

    /// Over an already-connected client: the way to use TLS or a connection
    /// the application already manages. The store can't replace a client it
    /// didn't make: once its connection drops, every later call fails.
    pub async fn from_client(
        client: Client,
        options: PostgresMemoryOptions,
    ) -> Result<Self, MemoryStoreError> {
        options.check()?;
        let link = Link::over(client, &options.link_options());
        Self::prepared(link, options).await
    }

    async fn prepared(
        link: Link,
        options: PostgresMemoryOptions,
    ) -> Result<Self, MemoryStoreError> {
        let store = Self {
            link,
            dimensions: options.dimensions,
            index: options.index,
            clock: options.clock,
            sql: Sql::new(&options.table, options.dimensions, options.index),
        };
        store.prepare(options.auto_migrate).await?;
        Ok(store)
    }

    /// Creates the table and its indexes and brings the `memory` component up
    /// to date, whatever the store's options say: what a deploy step runs
    /// when the application's stores don't migrate.
    pub async fn migrate(&self) -> Result<(), MemoryStoreError> {
        self.prepare(true).await
    }

    async fn prepare(&self, apply: bool) -> Result<(), MemoryStoreError> {
        self.link
            .retry
            .run(OPERATION, || self.prepare_once(apply))
            .await?;
        Ok(())
    }

    /// One attempt of [`Self::prepare`], in one transaction under the DDL
    /// lock every store opening the database takes.
    async fn prepare_once(&self, apply: bool) -> Result<(), Failure> {
        let mut client = self.link.client().await?;
        let tx = client.transaction().await?;
        match self.prepare_in(&tx, apply).await {
            Ok(()) => Ok(tx.commit().await?),
            Err(failure) => {
                // Rolled back now, not left for the connection to send: the
                // transaction holds the lock other stores wait for.
                let _ = tx.rollback().await;
                Err(failure)
            }
        }
    }

    /// The `memory` component is recorded once per database, but each table
    /// is created when missing: a second store on another table (other
    /// dimensions) gets its own in a database already at version 1.
    async fn prepare_in(&self, tx: &Transaction<'_>, apply: bool) -> Result<(), Failure> {
        let table = &self.sql.table;
        let steps = schema_versions::postgres_memory_steps(&[]);
        let known = schema_versions::known(schema_versions::MEMORY_VERSION, &steps);
        let found = locked_version(tx, schema_versions::MEMORY, known, apply).await?;
        let exists: bool = tx
            .query_one(&format!("SELECT to_regclass('{table}') IS NOT NULL"), &[])
            .await?
            .get(0);
        if !exists && !apply {
            return Err(Failure::fatal(format!(
                "the memory table '{table}' doesn't exist: run its migration"
            )));
        }
        if apply {
            // Every statement is `IF NOT EXISTS`: a table that lost an index
            // gets it back. The extension is installed only when missing, so
            // a role that may not install one opens a database that has it.
            let installed: bool = tx
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')",
                    &[],
                )
                .await?
                .get(0);
            if !installed {
                create_extension(tx).await?;
            }
            tx.batch_execute(&self.sql.ddl).await?;
        }
        if found < known {
            // Version 1 is the table, made above or already there.
            run_steps(
                tx,
                schema_versions::MEMORY,
                &steps,
                found.max(schema_versions::MEMORY_VERSION),
                known,
            )
            .await?;
        }
        let dimensions: Option<i32> = tx
            .query_opt(
                &format!(
                    "SELECT atttypmod FROM pg_attribute
                     WHERE attrelid = to_regclass('{table}') AND attname = 'embedding' AND NOT attisdropped"
                ),
                &[],
            )
            .await?
            .map(|row| row.get(0));
        match dimensions {
            None => {
                return Err(Failure::fatal(format!(
                    "the memory table '{table}' has no embedding column"
                )));
            }
            Some(found) if i64::from(found) != self.dimensions as i64 => {
                return Err(Failure::fatal(format!(
                    "memory was created with {found} dimensions, and this store was opened with {}",
                    self.dimensions
                )));
            }
            Some(_) => {}
        }
        if apply && !self.sql.hnsw.is_empty() {
            tx.batch_execute(&self.sql.hnsw).await?;
        }
        Ok(())
    }

    async fn upsert_rows(&self, records: Vec<MemoryRecord>) -> Result<(), MemoryStoreError> {
        for record in &records {
            record.validate(self.dimensions)?;
        }
        if records.is_empty() {
            return Ok(());
        }
        // One time for every attempt, so a repeated attempt writes the same.
        let now = timestamp_text((self.clock)());
        let rows: Vec<UpsertRow> = records.iter().map(UpsertRow::of).collect();
        observed(
            STORE,
            "upsert",
            self.link
                .retry
                .run(OPERATION, || self.upsert_once(&rows, &now)),
        )
        .await?;
        Ok(())
    }

    async fn upsert_once(&self, rows: &[UpsertRow], now: &String) -> Result<(), Failure> {
        let mut client = self.link.client().await?;
        let statement = client.statement(&self.sql.upsert).await?;
        if let [row] = rows {
            client.execute(&statement, &row.params(now)).await?;
            return Ok(());
        }
        let tx = client.transaction().await?;
        for row in rows {
            tx.execute(&statement, &row.params(now)).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn get_row(&self, key: &MemoryKey) -> Result<Option<MemoryRecord>, MemoryStoreError> {
        key.validate()?;
        Ok(observed(
            STORE,
            "get",
            self.link.retry.run(OPERATION, || async {
                let client = self.link.client().await?;
                let statement = client.statement(&self.sql.get).await?;
                client
                    .query_opt(&statement, &[&key.namespace, &key.tenant, &key.id])
                    .await?
                    .map(|row| read_record(&row))
                    .transpose()
            }),
        )
        .await?)
    }

    async fn delete_row(&self, key: &MemoryKey) -> Result<bool, MemoryStoreError> {
        key.validate()?;
        Ok(observed(
            STORE,
            "delete",
            self.link.retry.run(OPERATION, || async {
                let client = self.link.client().await?;
                let statement = client.statement(&self.sql.delete).await?;
                let deleted = client
                    .execute(&statement, &[&key.namespace, &key.tenant, &key.id])
                    .await?;
                Ok(deleted > 0)
            }),
        )
        .await?)
    }

    async fn search_rows(
        &self,
        search: &MemorySearch,
    ) -> Result<Vec<MemoryMatch>, MemoryStoreError> {
        search.validate(self.dimensions)?;
        let vector = vector_text(&search.embedding.vector);
        let exclude: Vec<&str> = search.exclude.iter().map(String::as_str).collect();
        let metadata = pairs_json(&search.metadata);
        let (limit, candidates) = (search.limit as i64, 2 * search.limit as i64);
        let params: [&(dyn ToSql + Sync); 8] = [
            &search.namespace,
            &search.tenant,
            &search.embedding.model,
            &vector,
            &exclude,
            &metadata,
            &limit,
            &candidates,
        ];
        let matches = observed(
            STORE,
            "search",
            self.link.retry.run(OPERATION, || async {
                let rows = self.search_once(&params).await?;
                let mut matches = Vec::with_capacity(rows.len());
                for row in &rows {
                    let similarity: f64 = row.try_get("similarity")?;
                    // Rows come most similar first: the first below the
                    // minimum ends the result.
                    if search.min_similarity.is_some_and(|min| similarity < min) {
                        break;
                    }
                    matches.push(MemoryMatch {
                        record: read_record(row)?,
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

    async fn search_once(&self, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Failure> {
        let mut client = self.link.client().await?;
        if self.index == MemoryIndex::None {
            // The exact search has no `$8`.
            let statement = client.statement(&self.sql.search).await?;
            return Ok(client.query(&statement, &params[..7]).await?);
        }
        let statement = client.statement(&self.sql.search_hnsw).await?;
        let tx = client.transaction().await?;
        // pgvector 0.8: the index scan goes on until the filters have let
        // enough rows through, so a filter can't starve the limit.
        tx.batch_execute("SET LOCAL hnsw.iterative_scan = relaxed_order")
            .await?;
        let rows = tx.query(&statement, params).await?;
        tx.commit().await?;
        Ok(rows)
    }

    async fn list_rows(&self, query: &MemoryQuery) -> Result<MemoryPage, MemoryStoreError> {
        let after = query.validate()?;
        let metadata = pairs_json(&query.metadata);
        // One more than the page, to tell whether another follows.
        let limit = query.limit as i64 + 1;
        let records = observed(
            STORE,
            "list",
            self.link.retry.run(OPERATION, || async {
                let client = self.link.client().await?;
                let statement = client.statement(&self.sql.list).await?;
                client
                    .query(
                        &statement,
                        &[&query.namespace, &query.tenant, &metadata, &after, &limit],
                    )
                    .await?
                    .iter()
                    .map(read_record)
                    .collect::<Result<Vec<_>, _>>()
            }),
        )
        .await?;
        Ok(MemoryPage::of(records, query.limit))
    }

    async fn purge_rows(&self, purge: &MemoryPurge) -> Result<usize, MemoryStoreError> {
        purge.validate()?;
        let older_than = purge.older_than_text();
        let limit = i64::try_from(purge.limit).unwrap_or(i64::MAX);
        let deleted = observed(
            STORE,
            "purge",
            self.link.retry.run(OPERATION, || async {
                let client = self.link.client().await?;
                let statement = client.statement(&self.sql.purge).await?;
                Ok(client
                    .execute(
                        &statement,
                        &[&older_than, &purge.namespace, &purge.tenant, &limit],
                    )
                    .await?)
            }),
        )
        .await?;
        Ok(deleted as usize)
    }
}

/// `CREATE EXTENSION IF NOT EXISTS vector`, refused in words that say what is
/// missing when the server has no pgvector or the role may not install it.
async fn create_extension(tx: &Transaction<'_>) -> Result<(), Failure> {
    match tx
        .batch_execute("CREATE EXTENSION IF NOT EXISTS vector")
        .await
    {
        Ok(()) => Ok(()),
        Err(err) => {
            let reason = err.as_db_error().map(|db| db.message().to_string());
            let failure = Failure::from(err);
            if failure.transient {
                return Err(failure);
            }
            Err(Failure::fatal(format!(
                "memory needs pgvector: CREATE EXTENSION vector failed ({})",
                reason.unwrap_or(failure.message)
            )))
        }
    }
}

/// A record's upsert parameters, made once for every attempt.
struct UpsertRow {
    namespace: String,
    tenant: String,
    id: String,
    model: String,
    vector: String,
    content: Option<Value>,
    text: Option<String>,
    metadata: Value,
}

impl UpsertRow {
    fn of(record: &MemoryRecord) -> Self {
        Self {
            namespace: record.namespace.clone(),
            tenant: record.tenant.clone(),
            id: record.id.clone(),
            model: record.embedding.model.clone(),
            vector: vector_text(&record.embedding.vector),
            content: record.content.clone().map(Value::Object),
            text: record.text.clone(),
            metadata: pairs_json(&record.metadata),
        }
    }

    fn params<'a>(&'a self, now: &'a String) -> [&'a (dyn ToSql + Sync); 9] {
        [
            &self.namespace,
            &self.tenant,
            &self.id,
            &self.model,
            &self.vector,
            &self.content,
            &self.text,
            &self.metadata,
            now,
        ]
    }
}

fn pairs_json(pairs: &BTreeMap<String, String>) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect(),
    )
}

/// A row of [`COLUMNS`] as a record.
fn read_record(row: &Row) -> Result<MemoryRecord, Failure> {
    let id: String = row.try_get("id")?;
    let bad = |what: &str| Failure::fatal(format!("memory record '{id}' has {what}"));
    let vector = parse_vector_text(&row.try_get::<_, String>("embedding_text")?)
        .map_err(|_| bad("an embedding that isn't a vector"))?;
    let content = match row.try_get::<_, Option<Value>>("content")? {
        None | Some(Value::Null) => None,
        Some(Value::Object(content)) => Some(content),
        Some(_) => return Err(bad("content that isn't a JSON object")),
    };
    let metadata = match row.try_get::<_, Value>("metadata")? {
        Value::Object(pairs) => pairs
            .into_iter()
            .map(|(key, value)| match value {
                Value::String(value) => Ok((key, value)),
                _ => Err(bad("metadata that isn't string pairs")),
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?,
        _ => return Err(bad("metadata that isn't string pairs")),
    };
    let time = |column: &str| -> Result<DateTime<Utc>, Failure> {
        let text: String = row.try_get(column)?;
        DateTime::parse_from_rfc3339(&text)
            .map(|time| time.with_timezone(&Utc))
            .map_err(|_| bad(&format!("a {column} that isn't a timestamp: '{text}'")))
    };
    let mut record = MemoryRecord::new(
        row.try_get::<_, String>("namespace")?,
        row.try_get::<_, String>("tenant")?,
        id.clone(),
        Embedding::new(row.try_get::<_, String>("model")?, vector),
    );
    record.content = content;
    record.text = row.try_get("text")?;
    record.metadata = metadata;
    record.created_at = Some(time("created_at")?);
    record.updated_at = Some(time("updated_at")?);
    Ok(record)
}

/// A vector as pgvector reads it: `[1,0.5,-0.25]`, each value the shortest
/// decimal that reads back as the same `f32`.
fn vector_text(vector: &[f32]) -> String {
    let mut text = String::with_capacity(vector.len() * 8 + 2);
    text.push('[');
    for (index, value) in vector.iter().enumerate() {
        if index > 0 {
            text.push(',');
        }
        text.push_str(&value.to_string());
    }
    text.push(']');
    text
}

/// A vector as pgvector writes it (`embedding::text`), exponents included.
fn parse_vector_text(text: &str) -> Result<Vec<f32>, std::num::ParseFloatError> {
    let inner = text
        .trim()
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(text)
        .trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    inner.split(',').map(|value| value.trim().parse()).collect()
}

impl MemoryStore for PostgresMemoryStore {
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

impl MemoryRetention for PostgresMemoryStore {
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

    #[test]
    fn a_vector_is_written_with_the_shortest_digits() {
        assert_eq!(vector_text(&[1.0, 0.0, -0.0]), "[1,0,-0]");
        assert_eq!(vector_text(&[0.5, -0.25, 0.1]), "[0.5,-0.25,0.1]");
        assert_eq!(vector_text(&[1e-7, 3.4028235e38]), {
            let (a, b) = (1e-7f32.to_string(), 3.4028235e38f32.to_string());
            format!("[{a},{b}]")
        });
        assert_eq!(vector_text(&[]), "[]");
    }

    #[test]
    fn a_vector_reads_back_as_the_same_floats() {
        let vectors: [&[f32]; 4] = [
            &[1.0, 0.0, 0.0],
            &[0.1, -0.333_333_34, 123_456.79],
            &[f32::MIN_POSITIVE, f32::MAX, -f32::MAX],
            &[1e-45, 7e-39],
        ];
        for vector in vectors {
            assert_eq!(parse_vector_text(&vector_text(vector)).unwrap(), vector);
        }
    }

    #[test]
    fn pgvectors_own_text_is_read() {
        // pgvector writes exponents and no spaces; spaces are tolerated.
        assert_eq!(
            parse_vector_text("[1,-0.5,1e-07,2.5e+10]").unwrap(),
            vec![1.0, -0.5, 1e-7, 2.5e10]
        );
        assert_eq!(parse_vector_text("[ 1, 2 ]").unwrap(), vec![1.0, 2.0]);
        assert_eq!(parse_vector_text("[]").unwrap(), Vec::<f32>::new());
        assert!(parse_vector_text("[1,x]").is_err());
    }

    #[test]
    fn options_no_store_can_open_with_are_refused() {
        let refusal = |options: PostgresMemoryOptions| options.check().unwrap_err().0;
        assert_eq!(
            refusal(PostgresMemoryOptions::new(0)),
            "a memory store has 1 to 16000 dimensions, not 0"
        );
        assert_eq!(
            refusal(PostgresMemoryOptions::new(3072).with_index(MemoryIndex::hnsw())),
            "an HNSW index takes at most 2000 dimensions, not 3072"
        );
        for table in [
            "Memory",
            "memory-records",
            "1memory",
            "a.b.c",
            "",
            "memory;drop",
        ] {
            assert_eq!(
                refusal(PostgresMemoryOptions::new(3).with_table(table)),
                format!(
                    "the memory table '{table}' isn't a lowercase SQL name (name or schema.name)"
                )
            );
        }
        assert!(
            PostgresMemoryOptions::new(3)
                .with_table("ops.memory_768")
                .check()
                .is_ok()
        );
        assert!(PostgresMemoryOptions::new(16_000).check().is_ok());
    }

    #[test]
    fn an_index_is_named_after_its_table_in_the_tables_schema() {
        let sql = Sql::new("ops.facts_768", 768, MemoryIndex::hnsw());
        assert!(
            sql.ddl
                .contains("CREATE INDEX IF NOT EXISTS facts_768_scope ON ops.facts_768")
        );
        assert!(sql.ddl.contains("embedding   vector(768) NOT NULL"));
        assert!(sql.hnsw.starts_with(
            "CREATE INDEX IF NOT EXISTS facts_768_embedding_hnsw ON ops.facts_768 USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)"
        ));
        assert!(
            Sql::new(DEFAULT_TABLE, 3, MemoryIndex::None)
                .hnsw
                .is_empty()
        );
    }
}
