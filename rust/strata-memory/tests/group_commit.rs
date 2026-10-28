//! Group commit (`design/41-Checkpoint-Efficiency.md`, SG-15) in the SQLite
//! and Postgres stores, and the Postgres store's connection pool. The queue on
//! its own is tested in `src/group_commit.rs`. The xUnit twin is
//! `GroupCommitTests`.
//!
//! The Postgres tests need `STRATA_TEST_POSTGRES_URL` (see `common`).

mod common;

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common::{PgSandbox, backend_url, unique_name};
use datalium_strata::checkpoint::{CheckpointQuery, CheckpointStore, LeaseAttempt, ThreadLease};
use datalium_strata::schema::{Checkpoint, CheckpointStatus, SCHEMA_VERSION};
use datalium_strata::store::LeasedSave;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use datalium_strata_memory::group_commit::FLUSH_SIZE_METRIC;
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::{GroupCommit, PostgresStoreOptions, SqliteStoreOptions};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use serde_json::json;

/// How long a test waits for something that should happen at once.
const PATIENCE: Duration = Duration::from_secs(30);

fn saved(thread_id: &str, n: u32) -> Checkpoint {
    Checkpoint {
        schema_version: SCHEMA_VERSION.to_string(),
        thread_id: thread_id.to_string(),
        status: CheckpointStatus::Interrupted,
        suspended_node: Some("Approve".to_string()),
        next_node: Some("Send".to_string()),
        frozen_state: json!({ "n": n, "note": "say \"hi\" 日本語" }),
        metadata: None,
        created_at: format!("2026-10-15T00:{:02}:{:02}Z", n / 60, n % 60),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

/// The seconds of each checkpoint in `thread_id`'s log, in order.
async fn log(store: &dyn CheckpointQuery, thread_id: &str) -> Vec<String> {
    store
        .get_log(thread_id, None, 1000)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint.created_at[17..19].to_string())
        .collect()
}

/// A fault hook that holds the first flush reaching `point` after
/// [`Hold::arm`] until [`Hold::release`], so the saves made meanwhile all wait
/// for the next flush. It blocks the thread the flush runs on: in a runtime
/// worker, through `block_in_place`, so the worker's other duties (the timer
/// among them) move to another thread meanwhile.
#[derive(Clone)]
struct Hold {
    point: &'static str,
    armed: Arc<AtomicBool>,
    state: Arc<(Mutex<(bool, bool)>, Condvar)>,
}

impl Hold {
    fn new(point: &'static str) -> Self {
        Self {
            point,
            armed: Arc::new(AtomicBool::new(false)),
            state: Arc::new((Mutex::new((false, false)), Condvar::new())),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn hook(&self) -> impl Fn(&str) -> bool + Send + Sync + 'static {
        let hold = self.clone();
        move |reached| {
            if reached == hold.point && hold.armed.swap(false, Ordering::SeqCst) {
                let wait = || {
                    let (lock, changed) = &*hold.state;
                    let mut state = lock.lock().unwrap();
                    state.0 = true;
                    changed.notify_all();
                    let (state, _) = changed
                        .wait_timeout_while(state, PATIENCE, |state| !state.1)
                        .unwrap();
                    assert!(state.1, "the held flush was never released");
                };
                match tokio::runtime::Handle::try_current() {
                    Ok(_) => tokio::task::block_in_place(wait),
                    // The blocking pool, where the SQLite store flushes.
                    Err(_) => wait(),
                }
            }
            false
        }
    }

    /// Waits until a flush is held.
    async fn held(&self) {
        let started = std::time::Instant::now();
        while !self.state.0.lock().unwrap().0 {
            assert!(started.elapsed() < PATIENCE, "no flush was held");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    fn release(&self) {
        let (lock, changed) = &*self.state;
        lock.lock().unwrap().1 = true;
        changed.notify_all();
    }
}

fn one_at_a_time() -> GroupCommit {
    GroupCommit::new().with_max_concurrent_flushes(1)
}

fn sqlite(options: SqliteStoreOptions) -> Arc<SqliteCheckpointStore> {
    Arc::new(SqliteCheckpointStore::open_in_memory_with(options).unwrap())
}

async fn postgres(
    sandbox: &PgSandbox,
    options: PostgresStoreOptions,
) -> Arc<PostgresCheckpointStore> {
    Arc::new(
        PostgresCheckpointStore::connect_with(&sandbox.url(None), options)
            .await
            .unwrap(),
    )
}

async fn acquire(store: &dyn CheckpointStore, thread_id: &str, owner: &str) -> ThreadLease {
    match store
        .try_acquire_lease(thread_id, owner, Duration::from_secs(300))
        .await
        .unwrap()
    {
        LeaseAttempt::Acquired(lease) => lease,
        LeaseAttempt::Held(holder) => panic!("held by {holder:?}"),
    }
}

/// Runs `call` on a task of its own, so the test can go on while it waits.
fn spawned<T: Send + 'static>(
    call: impl std::future::Future<Output = T> + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    tokio::spawn(call)
}

async fn within<T>(task: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(PATIENCE, task)
        .await
        .expect("a save never finished")
        .unwrap()
}

/// One flush holding a conditional save that lost its race, a fenced save
/// whose lease was taken over, one whose lease is live, plain saves, two saves
/// of one thread, and a conditional save that wins after an earlier row of its
/// thread was refused: each gets its own answer, and every log is as if they
/// had been made one by one.
async fn each_row_answered<S>(store: Arc<S>, hold: Hold)
where
    S: CheckpointStore + CheckpointQuery + 'static,
{
    store.save(saved("cas", 0)).await.unwrap();
    store.save(saved("cas", 1)).await.unwrap();
    let stale = acquire(&*store, "fenced", "w1").await;
    assert!(store.release_lease(&stale).await.unwrap());
    let live = acquire(&*store, "fenced", "w2").await;
    assert_eq!(live.token, stale.token + 1);

    hold.arm();
    let blocker = spawned({
        let store = store.clone();
        async move { store.save(saved("blocker", 0)).await }
    });
    hold.held().await;
    // Each call is queued in this order before the next is made.
    macro_rules! queue {
        ($call:expr) => {{
            let store = store.clone();
            let task = spawned(async move {
                let store = store;
                #[allow(clippy::redundant_closure_call)]
                ($call)(store).await
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            task
        }};
    }
    let lost = queue!(|s: Arc<S>| async move { s.save_if_latest(saved("cas", 2), Some(0)).await });
    let fenced_out = queue!({
        let stale = stale.clone();
        move |s: Arc<S>| async move { s.save_under_lease(saved("fenced", 0), &stale, None).await }
    });
    let plain = queue!(|s: Arc<S>| async move { s.save(saved("plain", 0)).await });
    let pair_first = queue!(|s: Arc<S>| async move { s.save(saved("pair", 0)).await });
    let pair_second = queue!(|s: Arc<S>| async move { s.save(saved("pair", 1)).await });
    let wins = queue!(|s: Arc<S>| async move { s.save_if_latest(saved("cas", 3), Some(1)).await });
    let fenced_in = queue!({
        let live = live.clone();
        move |s: Arc<S>| async move { s.save_under_lease(saved("fenced", 1), &live, None).await }
    });
    let blocker_again = queue!(|s: Arc<S>| async move { s.save(saved("blocker", 1)).await });
    hold.release();

    within(blocker).await.unwrap();
    assert!(!within(lost).await.unwrap());
    assert_eq!(within(fenced_out).await.unwrap(), LeasedSave::LeaseLost);
    within(plain).await.unwrap();
    within(pair_first).await.unwrap();
    within(pair_second).await.unwrap();
    assert!(within(wins).await.unwrap());
    assert_eq!(within(fenced_in).await.unwrap(), LeasedSave::Saved);
    within(blocker_again).await.unwrap();

    assert_eq!(log(&*store, "cas").await, ["00", "01", "03"]);
    assert_eq!(log(&*store, "fenced").await, ["01"]);
    assert_eq!(log(&*store, "pair").await, ["00", "01"]);
    assert_eq!(log(&*store, "blocker").await, ["00", "01"]);
    assert_eq!(log(&*store, "plain").await, ["00"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_answers_each_row_of_a_flush_on_its_own() {
    let hold = Hold::new("flush:start");
    let store = sqlite(
        SqliteStoreOptions::new()
            .with_group_commit(one_at_a_time())
            .with_fault(hold.hook()),
    );
    each_row_answered(store, hold).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_answers_each_row_of_a_flush_on_its_own() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "rows");
    let hold = Hold::new("save:before-commit");
    let store = postgres(
        &sandbox,
        PostgresStoreOptions::new()
            .with_group_commit(one_at_a_time())
            .with_fault(hold.hook()),
    )
    .await;
    each_row_answered(store, hold).await;
}

/// Saves of many threads from many writers at once: every log whole and in
/// order, and flushes that took more than one save. The flushes' sizes are
/// read on this thread's recorder: a current-thread runtime runs the flushes'
/// tasks here.
async fn concurrent_writers<S>(store: Arc<S>)
where
    S: CheckpointStore + CheckpointQuery + 'static,
{
    const WRITERS: usize = 24;
    const SAVES: u32 = 20;
    let recorder: &'static DebuggingRecorder = Box::leak(Box::new(DebuggingRecorder::new()));
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(recorder);

    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let store = store.clone();
            tokio::spawn(async move {
                for n in 0..SAVES {
                    store.save(saved(&format!("w{writer}"), n)).await.unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        within(writer).await;
    }

    let every: Vec<String> = (0..SAVES).map(|n| format!("{n:02}")).collect();
    for writer in 0..WRITERS {
        assert_eq!(log(&*store, &format!("w{writer}")).await, every);
    }
    let sizes: Vec<f64> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == FLUSH_SIZE_METRIC)
        .flat_map(|(_, _, _, value)| match value {
            DebugValue::Histogram(values) => values.into_iter().map(|v| v.into_inner()).collect(),
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(sizes.iter().sum::<f64>() as usize, WRITERS * SAVES as usize);
    assert!(
        sizes.iter().any(|&size| size > 1.0),
        "every flush took one save: {sizes:?}"
    );
}

#[tokio::test]
async fn sqlite_keeps_every_log_whole_under_concurrent_writers() {
    concurrent_writers(sqlite(
        SqliteStoreOptions::new().with_group_commit(GroupCommit::new()),
    ))
    .await;
}

#[tokio::test]
async fn postgres_keeps_every_log_whole_under_concurrent_writers() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "writers");
    concurrent_writers(
        postgres(
            &sandbox,
            PostgresStoreOptions::new().with_group_commit(GroupCommit::new()),
        )
        .await,
    )
    .await;
}

/// Fails `point` the first `times` times it is reached.
fn fault_at(point: &'static str, times: i32) -> impl Fn(&str) -> bool + Send + Sync + 'static {
    let left = AtomicI32::new(times);
    move |reached| reached == point && left.fetch_sub(1, Ordering::SeqCst) > 0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_looks_up_each_row_of_a_flush_whose_commit_may_have_happened() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "in_doubt");
    let hold = Hold::new("save:before-commit");
    let (held, after_commit) = (hold.hook(), fault_at("save:after-commit", 1));
    let store = postgres(
        &sandbox,
        PostgresStoreOptions::new()
            .with_group_commit(one_at_a_time())
            .with_fault(move |point| held(point) || after_commit(point)),
    )
    .await;
    store.save(saved("cas", 0)).await.unwrap();

    hold.arm();
    let blocker = spawned({
        let store = store.clone();
        async move { store.save(saved("blocker", 0)).await }
    });
    hold.held().await;
    let mut calls = Vec::new();
    for (thread, n, expected) in [
        ("cas", 1, Some(0)),
        ("cas", 2, Some(0)),
        ("plain", 0, None),
        ("plain", 1, None),
    ] {
        let store = store.clone();
        calls.push(spawned(async move {
            store.save_if_latest(saved(thread, n), expected).await
        }));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    hold.release();

    within(blocker).await.unwrap();
    let answers: Vec<bool> = {
        let mut answers = Vec::new();
        for call in calls {
            answers.push(within(call).await.unwrap());
        }
        answers
    };
    // The blocker's flush failed after its commit (the fault fires once, on
    // the first flush to reach it): looked up, not written again. Then the
    // queued flush: the winner and the plain saves written once, the loser
    // refused.
    assert_eq!(answers, [true, false, true, true]);
    assert_eq!(log(&*store, "blocker").await, ["00"]);
    assert_eq!(log(&*store, "cas").await, ["00", "01"]);
    assert_eq!(log(&*store, "plain").await, ["00", "01"]);
}

#[tokio::test]
async fn postgres_repeats_a_flush_that_failed_before_its_commit_and_writes_once() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "before_commit");
    let store = postgres(
        &sandbox,
        PostgresStoreOptions::new()
            .with_group_commit(GroupCommit::new())
            .with_fault(fault_at("save:before-commit", 1)),
    )
    .await;

    store.save(saved("t", 0)).await.unwrap();

    assert_eq!(log(&*store, "t").await, ["00"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_fails_only_the_row_the_database_refuses() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "bad_row");
    let hold = Hold::new("save:before-commit");
    let store = postgres(
        &sandbox,
        PostgresStoreOptions::new()
            .with_group_commit(one_at_a_time())
            .with_fault(hold.hook()),
    )
    .await;

    hold.arm();
    let blocker = spawned({
        let store = store.clone();
        async move { store.save(saved("blocker", 0)).await }
    });
    hold.held().await;
    let mut bad = saved("bad", 0);
    // JSONB can't hold a NUL character: the database refuses this row, and it
    // alone.
    bad.frozen_state = json!({ "text": "a\u{0}b" });
    let mut calls = Vec::new();
    for checkpoint in [saved("good", 0), bad, saved("also", 0)] {
        let store = store.clone();
        calls.push(spawned(async move { store.save(checkpoint).await }));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    hold.release();

    within(blocker).await.unwrap();
    let mut results = Vec::new();
    for call in calls {
        results.push(within(call).await);
    }
    assert!(results[0].is_ok());
    assert!(results[1].is_err(), "{:?}", results[1]);
    assert!(results[2].is_ok());
    assert_eq!(log(&*store, "good").await, ["00"]);
    assert_eq!(log(&*store, "also").await, ["00"]);
    assert!(log(&*store, "bad").await.is_empty());
}

#[tokio::test]
async fn without_group_commit_a_store_saves_as_before() {
    assert!(
        !SqliteCheckpointStore::open_in_memory()
            .unwrap()
            .group_commits()
    );
    assert!(
        SqliteCheckpointStore::open_in_memory_with(
            SqliteStoreOptions::new().with_group_commit(GroupCommit::new())
        )
        .unwrap()
        .group_commits()
    );
    assert!(
        SqliteCheckpointStore::open_in_memory_with(
            SqliteStoreOptions::new().with_group_commit(GroupCommit::new().with_max_batch(0))
        )
        .is_err()
    );
}

// --- The pool ---

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_postgres_store_uses_several_connections_up_to_its_pool_size() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "pool");
    let name = unique_name("pool");
    let admin = sandbox.client().await;
    let store = Arc::new(
        PostgresCheckpointStore::connect_with(
            &sandbox.url(Some(&name)),
            PostgresStoreOptions::new().with_pool_size(3),
        )
        .await
        .unwrap(),
    );

    let writers: Vec<_> = (0..12)
        .map(|writer| {
            let store = store.clone();
            tokio::spawn(async move {
                for n in 0..10 {
                    store.save(saved(&format!("p{writer}"), n)).await.unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        within(writer).await;
    }

    let connections: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        (2..=3).contains(&connections),
        "{connections} connections for a pool of 3"
    );
    assert!(
        PostgresCheckpointStore::connect_with(
            &sandbox.url(None),
            PostgresStoreOptions::new().with_pool_size(0)
        )
        .await
        .is_err()
    );
}
