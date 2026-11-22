//! Samples for <https://stratamesh.dev/guides/ai/long-term-memory/>.
//!
//! The stores sample needs `STRATA_TEST_POSTGRES_URL` (with pgvector) and `STRATA_TEST_REDIS_URL` (Redis 8).
//! Without them it prints `SKIPPED` and passes, unless `STRATA_REQUIRE_BACKENDS=1`. Its Postgres tables go in
//! a schema of their own and its Redis keys under a prefix of their own, both removed afterwards.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use datalium_strata::CancellationToken;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::StateGraph;
use datalium_strata::memory::{InMemoryMemoryStore, MemoryStore};
use datalium_strata_ai::{Memory, MemoryEntry, RecallOptions};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use datalium_strata_memory::{SqliteMemoryOptions, SqliteMemoryStore};
use serde::Serialize;

type TestResult = Result<(), Box<dyn Error>>;

// docs:begin guides/ai/long-term-memory/word-hash
use datalium_strata::BoxFuture;
use datalium_strata_ai::{ChatError, Embedder, EmbeddingRequest, EmbeddingResponse};

/// Each word of four letters or more adds 1 to one of 64 buckets.
struct WordHash;

impl WordHash {
    const DIMENSIONS: usize = 64;

    fn vector(text: &str) -> Vec<f32> {
        let mut vector = vec![0.0f32; Self::DIMENSIONS];
        let lower = text.to_lowercase();
        let words = lower
            .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
            .filter(|word| word.len() >= 4);
        for word in words {
            // FNV-1a: the same bucket in every build and process.
            let mut hash: u32 = 2_166_136_261;
            for byte in word.bytes() {
                hash = (hash ^ u32::from(byte)).wrapping_mul(16_777_619);
            }
            vector[(hash % Self::DIMENSIONS as u32) as usize] += 1.0;
        }
        vector[0] += 0.01; // never all zeros: the store refuses a vector with no direction
        vector
    }
}

// The label: test/word-hash.
impl Embedder for WordHash {
    fn provider(&self) -> &str {
        "test"
    }

    fn default_model(&self) -> Option<&str> {
        Some("word-hash")
    }

    fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> BoxFuture<'_, Result<EmbeddingResponse, ChatError>> {
        let vectors = request
            .inputs
            .iter()
            .map(|text| Self::vector(text))
            .collect();
        Box::pin(async move { Ok(EmbeddingResponse::new(vectors)) })
    }
}
// docs:end guides/ai/long-term-memory/word-hash

// docs:begin guides/ai/long-term-memory/tool
use datalium_strata::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, StrataSchema)]
struct RecallArgs {
    symptoms: String,
}

#[strata_tool(
    name = "incidents.recall",
    description = "Past incidents most like the given symptoms, with what fixed them."
)]
async fn incidents_recall(args: RecallArgs, ctx: ToolCallContext) -> Result<String, String> {
    let memory = ctx
        .service::<Memory>()
        .ok_or("no Memory service in the run")?;
    let tenant = ctx.metadata().get("tenant").cloned().unwrap_or_default();
    let options = RecallOptions::new().tenant(tenant).limit(3);
    let matches = memory
        .recall("incidents", &args.symptoms, options, None)
        .await
        .map_err(|error| error.to_string())?;
    Ok(matches
        .iter()
        .map(|m| {
            let action = m
                .record
                .content
                .as_ref()
                .and_then(|c| c.get("action")?.as_str());
            format!(
                "{} ({:.2}): {}",
                m.record.id,
                m.similarity,
                action.unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("\n"))
}
// docs:end guides/ai/long-term-memory/tool

/// The four incidents the remember sample stores, two of them elided on the page.
fn incidents() -> Vec<MemoryEntry> {
    let entry =
        |tenant: &str, id: &str, service: &str, symptoms: &str, root_cause: &str, action: &str| {
            let content = serde_json::json!({ "root_cause": root_cause, "action": action });
            MemoryEntry::new("incidents", id, symptoms)
                .with_tenant(tenant)
                .with_content(content.as_object().cloned().unwrap_or_default())
                .with_metadata("service", service)
        };
    vec![
        entry(
            "acme",
            "inc-101",
            "checkout",
            "Checkout pods crash looping, out of memory after deploy",
            "a memory leak in v2.3",
            "roll back the deploy",
        ),
        entry(
            "acme",
            "inc-103",
            "search",
            "Search pods out of memory during the nightly reindex",
            "a reindex batch too large",
            "halve the batch",
        ),
        entry(
            "acme",
            "inc-102",
            "checkout",
            "Checkout latency spike, database connection pool exhausted",
            "a connection pool too small for the sale",
            "raise the pool size",
        ),
        entry(
            "globex",
            "inc-101",
            "checkout",
            "Checkout pods crash looping, out of memory after deploy",
            "a noisy neighbour on the node",
            "move the pods to another node pool",
        ),
    ]
}

#[test]
fn create_an_embedder() -> TestResult {
    let api_key = "sk-docs-snippets";
    // docs:begin guides/ai/long-term-memory/embedder
    use std::sync::Arc;

    use datalium_strata_ai::{Embedder, OpenAiEmbedder, OpenAiOptions};

    // OpenAI
    let embedder: Arc<dyn Embedder> = Arc::new(OpenAiEmbedder::new(
        OpenAiOptions::new(api_key),
        "text-embedding-3-small",
    ));

    // Ollama
    let local: Arc<dyn Embedder> = Arc::new(OpenAiEmbedder::new(
        OpenAiOptions::ollama("http://localhost:11434"),
        "nomic-embed-text",
    ));
    // docs:end guides/ai/long-term-memory/embedder

    // The labels the page gives: <provider>/<model>.
    let store = Arc::new(InMemoryMemoryStore::new(1536));
    assert_eq!(
        Memory::new(store.clone(), embedder).label()?,
        "openai/text-embedding-3-small"
    );
    assert_eq!(
        Memory::new(store, local).label()?,
        "ollama/nomic-embed-text"
    );
    Ok(())
}

#[test]
fn create_the_helper() -> TestResult {
    let store = Arc::new(InMemoryMemoryStore::new(WordHash::DIMENSIONS));
    let embedder = Arc::new(WordHash);
    // docs:begin guides/ai/long-term-memory/helper
    use datalium_strata_ai::Memory;

    let memory = Memory::new(store.clone(), embedder);
    // docs:end guides/ai/long-term-memory/helper

    assert_eq!(memory.label()?, "test/word-hash");
    Ok(())
}

#[tokio::test]
async fn remember_recall_and_use_memory_in_a_node() -> TestResult {
    let store = Arc::new(InMemoryMemoryStore::new(WordHash::DIMENSIONS));
    let memory = Memory::new(store, Arc::new(WordHash));
    // The tool below needs the memory after the graph took its copy.
    let tool_memory = memory.clone();

    // docs:begin guides/ai/long-term-memory/remember
    use datalium_strata_ai::MemoryEntry;
    use serde_json::{Map, Value, json};

    fn content(value: Value) -> Map<String, Value> {
        match value {
            Value::Object(map) => map,
            _ => Map::new(),
        }
    }

    memory
        .remember_many(
            vec![
                MemoryEntry::new("incidents", "inc-101", "Checkout pods crash looping, out of memory after deploy")
                    .with_tenant("acme")
                    .with_content(content(json!({ "root_cause": "a memory leak in v2.3", "action": "roll back the deploy" })))
                    .with_metadata("service", "checkout"),
                MemoryEntry::new("incidents", "inc-103", "Search pods out of memory during the nightly reindex")
                    .with_tenant("acme")
                    .with_content(content(json!({ "root_cause": "a reindex batch too large", "action": "halve the batch" })))
                    .with_metadata("service", "search"),
                // docs:hide ... inc-102 for acme, and inc-101 for globex
                MemoryEntry::new("incidents", "inc-102", "Checkout latency spike, database connection pool exhausted")
                    .with_tenant("acme")
                    .with_content(content(json!({ "root_cause": "a connection pool too small for the sale", "action": "raise the pool size" })))
                    .with_metadata("service", "checkout"),
                MemoryEntry::new("incidents", "inc-101", "Checkout pods crash looping, out of memory after deploy")
                    .with_tenant("globex")
                    .with_content(content(json!({ "root_cause": "a noisy neighbour on the node", "action": "move the pods to another node pool" })))
                    .with_metadata("service", "checkout"),
                // docs:show
            ],
            None,
        )
        .await?;
    // docs:end guides/ai/long-term-memory/remember

    // docs:begin guides/ai/long-term-memory/recall
    use datalium_strata_ai::RecallOptions;

    let options = RecallOptions::new().tenant("acme").limit(3);
    let matches = memory
        .recall("incidents", "pods out of memory", options, None)
        .await?;
    for found in &matches {
        let action = found
            .record
            .content
            .as_ref()
            .and_then(|c| c.get("action"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        println!("{} {:.3} {action}", found.record.id, found.similarity);
    }
    // docs:end guides/ai/long-term-memory/recall

    // What the page says both languages print.
    let printed: Vec<String> = matches
        .iter()
        .map(|found| {
            let action = found
                .record
                .content
                .as_ref()
                .and_then(|c| c.get("action")?.as_str());
            format!(
                "{} {:.3} {}",
                found.record.id,
                found.similarity,
                action.unwrap_or_default()
            )
        })
        .collect();
    assert_eq!(
        printed,
        [
            "inc-103 0.577 halve the batch",
            "inc-101 0.535 roll back the deploy",
            "inc-102 0.000 raise the pool size",
        ]
    );

    let incident = Incident {
        id: "inc-201".to_string(),
        service: "checkout".to_string(),
        symptoms: "Checkout pods out of memory again after this morning's deploy".to_string(),
        ..Incident::default()
    };
    let globex_incident = incident.clone();

    // docs:begin guides/ai/long-term-memory/node
    let recall_memory = memory.clone();
    let desk = StateGraph::<Incident>::new()
        .add_async_fallible_node("Recall", move |incident: Incident, ctx: NodeContext| {
            let memory = recall_memory.clone();
            async move {
                // No tenant here: Memory reads it from the run's metadata.
                let options = RecallOptions::new()
                    .limit(3)
                    .min_similarity(0.5)
                    .exclude(incident.id.clone());
                let matches = memory
                    .recall("incidents", &incident.symptoms, options, &ctx)
                    .await?;
                let action = matches
                    .first()
                    .and_then(|m| m.record.content.as_ref()?.get("action")?.as_str())
                    .unwrap_or("page the on-call engineer")
                    .to_string();
                Ok(Incident {
                    similar: matches
                        .iter()
                        .map(|m| format!("{} ({:.2})", m.record.id, m.similarity))
                        .collect(),
                    action,
                    ..incident
                })
            }
        })
        .add_async_fallible_node("Remember", move |incident: Incident, ctx: NodeContext| {
            let memory = memory.clone();
            async move {
                let entry =
                    MemoryEntry::new("incidents", incident.id.clone(), incident.symptoms.clone())
                        .with_content(content(json!({ "action": incident.action })))
                        .with_metadata("service", incident.service.clone());
                memory.remember(entry, &ctx).await?;
                Ok(incident)
            }
        })
        .add_edge("Recall", "Remember")
        .compile()
        .expect("the graph is valid");

    let run = desk
        .run_with(
            "inc-201",
            incident,
            Arc::new(InMemoryCheckpointStore::new()),
            RunConfig::new().with_metadata("tenant", "acme"),
        )
        .await;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Incident {
        id: String,
        service: String,
        symptoms: String,
        similar: Vec<String>,
        action: String,
    }
    // docs:end guides/ai/long-term-memory/node

    // Each tenant finds only its own inc-101.
    let completed = |run: GraphExecutionResult<Incident>| match run {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => panic!("the run failed: {error:?}"),
        _ => panic!("the run paused, but this graph has nothing that pauses"),
    };
    let acme = completed(run);
    assert_eq!(
        (acme.similar, acme.action.as_str()),
        (vec!["inc-101 (0.67)".to_string()], "roll back the deploy")
    );
    let globex = completed(
        desk.run_with(
            "inc-201",
            globex_incident,
            Arc::new(InMemoryCheckpointStore::new()),
            RunConfig::new().with_metadata("tenant", "globex"),
        )
        .await,
    );
    assert_eq!(
        (globex.similar, globex.action.as_str()),
        (
            vec!["inc-101 (0.67)".to_string()],
            "move the pods to another node pool"
        )
    );

    // A recall with no tenant reads only the records whose tenant is "": none here.
    let no_tenant = tool_memory
        .recall(
            "incidents",
            "pods out of memory",
            RecallOptions::new(),
            None,
        )
        .await?;
    assert!(no_tenant.is_empty());

    // The tool, called for acme after the graph has run: inc-201 is the incident the graph remembered.
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(IncidentsRecallTool));
    let answer = tools
        .dispatch_with(
            "incidents.recall",
            r#"{"symptoms":"pods out of memory"}"#,
            ToolCallContext::new()
                .with_metadata("tenant", "acme")
                .with_service(Arc::new(tool_memory)),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(
        answer,
        "inc-103 (0.58): halve the batch\ninc-101 (0.53): roll back the deploy\ninc-201 (0.50): roll back the deploy"
    );
    Ok(())
}

#[tokio::test]
async fn forget() -> TestResult {
    // The records were written 400 days ago, by the store's clock.
    let written = Utc::now() - TimeDelta::days(400);
    let store = Arc::new(
        InMemoryMemoryStore::new(WordHash::DIMENSIONS).with_clock(Arc::new(move || written)),
    );
    Memory::new(store.clone(), Arc::new(WordHash))
        .remember_many(incidents(), None)
        .await?;

    // docs:begin guides/ai/long-term-memory/forget
    use chrono::{TimeDelta, Utc};
    use datalium_strata::memory::{MemoryKey, MemoryPurge, MemoryRetention, MemoryStore};

    let deleted = store
        .delete(&MemoryKey::new("incidents", "acme", "inc-102"))
        .await?;

    let cutoff = Utc::now() - TimeDelta::days(365);
    let purged = store
        .purge(&MemoryPurge::new(cutoff).namespace("incidents"))
        .await?;
    // docs:end guides/ai/long-term-memory/forget

    assert!(deleted);
    // acme's inc-101 and inc-103 and globex's inc-101: every tenant, as the purge names none.
    assert_eq!(purged, 3);
    Ok(())
}

/// The samples open `memory.db` in the working directory: one at a time, and removed afterwards.
static MEMORY_DB: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct RemoveMemoryDb;

impl Drop for RemoveMemoryDb {
    fn drop(&mut self) {
        for file in [
            "memory.db",
            "memory.db-wal",
            "memory.db-shm",
            "memory.db-journal",
        ] {
            let _ = std::fs::remove_file(file);
        }
    }
}

#[tokio::test]
async fn run_it_in_a_host() -> TestResult {
    let _one_at_a_time = MEMORY_DB.lock().await;
    let _remove = RemoveMemoryDb;
    let embedder = Arc::new(WordHash);

    // docs:begin guides/ai/long-term-memory/host
    use datalium_strata_host::{Host, MemoryRetentionDriver};

    let store = Arc::new(SqliteMemoryStore::open(
        "memory.db",
        SqliteMemoryOptions::new(1536),
    )?);
    let memory = Memory::new(store.clone(), embedder);

    let host = Host::new().retain_memory(
        store.clone(),
        MemoryRetentionDriver::new()
            .with_older_than(Duration::from_secs(365 * 24 * 60 * 60))
            .with_namespace("incidents"),
    );
    // docs:end guides/ai/long-term-memory/host

    assert_eq!(memory.label()?, "test/word-hash");
    assert_eq!(store.dimensions(), 1536);
    // The host accepts the driver: it would refuse one without older_than. Stopped before it starts.
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    host.run(shutdown).await?;
    Ok(())
}

// --- The stores, on the live backends. ---

const POSTGRES_VAR: &str = "STRATA_TEST_POSTGRES_URL";
const REDIS_VAR: &str = "STRATA_TEST_REDIS_URL";

/// The URL in `var`, or `None` (after saying so) if the test should skip.
fn backend_url(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var("STRATA_REQUIRE_BACKENDS").as_deref() != Ok("1"),
                "{var} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run"
            );
            eprintln!("SKIPPED: {var} is not set (start `docker compose up -d --wait`)");
            None
        }
    }
}

/// A name of this page's, unique to this process and call.
fn unique_name(label: &str) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!(
        "docs_long_term_memory_{}_{}_{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn postgres(url: &str) -> Result<tokio_postgres::Client, Box<dyn Error>> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

#[tokio::test]
async fn create_a_memory_store() -> TestResult {
    let (Some(server), Some(redis_url)) = (backend_url(POSTGRES_VAR), backend_url(REDIS_VAR))
    else {
        return Ok(());
    };
    let _one_at_a_time = MEMORY_DB.lock().await;
    let _remove = RemoveMemoryDb;

    // pgvector goes in `public`, once; the store's tables go in a schema of this test's own.
    let schema = unique_name("stores");
    let admin = postgres(&server).await?;
    let _ = admin
        .batch_execute("CREATE EXTENSION IF NOT EXISTS vector SCHEMA public")
        .await;
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;
    let separator = if server.contains('?') { '&' } else { '?' };
    let url = format!("{server}{separator}options=-c%20search_path%3D{schema}%2Cpublic");
    let prefix = unique_name("stores");

    let result: TestResult = async {
        // docs:begin guides/ai/long-term-memory/stores
        use datalium_strata::memory::InMemoryMemoryStore;
        use datalium_strata_memory::{
            MemoryIndex, PostgresMemoryOptions, PostgresMemoryStore, RedisMemoryOptions,
            RedisMemoryStore, SqliteMemoryOptions, SqliteMemoryStore,
        };

        // In memory: for tests and development. It's gone with the process.
        let store = Arc::new(InMemoryMemoryStore::new(1536));
        // docs:hide
        assert_eq!(store.dimensions(), 1536);
        // docs:show

        // SQLite with sqlite-vec, which the crate compiles in
        let store = Arc::new(SqliteMemoryStore::open(
            "memory.db",
            SqliteMemoryOptions::new(1536),
        )?);
        // docs:hide
        assert_eq!(store.dimensions(), 1536);
        drop(store);
        // docs:show

        // Postgres with pgvector, here with an HNSW index
        let options = PostgresMemoryOptions::new(1536).with_index(MemoryIndex::hnsw());
        let store = Arc::new(PostgresMemoryStore::connect(&url, options).await?);
        // docs:hide
        assert_eq!(store.dimensions(), 1536);
        // docs:show

        // Redis 8 or Redis Stack
        let options = RedisMemoryOptions::new(1536);
        // docs:hide
        let options = options.with_prefix(prefix.clone());
        // docs:show
        let store = Arc::new(RedisMemoryStore::connect(&redis_url, options).await?);
        // docs:end guides/ai/long-term-memory/stores

        assert_eq!(store.dimensions(), 1536);
        assert_eq!(store.index_name(), format!("{prefix}:memory:idx"));
        Ok(())
    }
    .await;

    // Remove what the stores made: the schema, and the index and keys under the prefix.
    admin
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await?;
    let mut redis = redis::Client::open(redis_url.as_str())?
        .get_multiplexed_async_connection()
        .await?;
    let _: Result<(), _> = redis::cmd("FT.DROPINDEX")
        .arg(format!("{prefix}:memory:idx"))
        .query_async(&mut redis)
        .await;
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query_async(&mut redis)
        .await?;
    if !keys.is_empty() {
        let _: () = redis::cmd("DEL").arg(keys).query_async(&mut redis).await?;
    }
    result
}
