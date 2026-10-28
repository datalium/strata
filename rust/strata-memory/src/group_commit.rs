//! Group commit (`design/41-Checkpoint-Efficiency.md`): saves that arrive
//! while flushes are running are flushed together, in one transaction, by the
//! Postgres and SQLite stores. There is no timer: a save that finds fewer than
//! [`GroupCommit::max_concurrent_flushes`] flushes running is flushed at once.
//! Each save keeps its own answer, one thread's saves keep their order, and a
//! save is answered only when its flush has committed. Mirrors the .NET
//! `GroupCommitOptions` and `GroupCommitQueue`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::CheckpointStoreError;
use datalium_strata::schema::Checkpoint;
use tokio::sync::oneshot;

use crate::retry::{Failure, STORE_LABEL};

/// The histogram of saves per flush, by [`STORE_LABEL`].
pub const FLUSH_SIZE_METRIC: &str = "strata.store.flush.size";

/// The histogram of how long a save waited for its flush to start, in
/// seconds, by [`STORE_LABEL`].
pub const FLUSH_WAIT_METRIC: &str = "strata.store.flush.wait";

/// The histogram of the JSON a store wrote for each saved checkpoint, in
/// bytes, by [`STORE_LABEL`] and [`ENCODING_LABEL`].
pub const CHECKPOINT_SIZE_METRIC: &str = "strata.store.checkpoint.size";

/// The label naming how a checkpoint was stored: `plain` until deltas and
/// offload exist.
pub const ENCODING_LABEL: &str = "strata.store.encoding";

/// Counter, `{blob}`: values a store offloaded to its blob store
/// (`design/41-Checkpoint-Efficiency.md`, "Telemetry"), by [`STORE_LABEL`]; a
/// blob it already had isn't counted.
pub const BLOBS_METRIC: &str = "strata.store.blobs";

/// Histogram, bytes: the JSON of each value a store offloaded, by
/// [`STORE_LABEL`].
pub const BLOB_SIZE_METRIC: &str = "strata.store.blob.size";

/// How a store flushes saves together. The default takes up to 256 saves a
/// flush and runs up to 4 flushes at once, each on a connection of its own.
/// Mirrors the .NET `GroupCommitOptions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommit {
    max_batch: usize,
    max_concurrent_flushes: usize,
}

impl Default for GroupCommit {
    fn default() -> Self {
        Self {
            max_batch: 256,
            max_concurrent_flushes: 4,
        }
    }
}

impl GroupCommit {
    pub fn new() -> Self {
        Self::default()
    }

    /// The most saves one flush takes. At least 1.
    pub fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch;
        self
    }

    /// How many flushes may run at once. At least 1; the SQLite store, which
    /// has one connection, always runs one.
    pub fn with_max_concurrent_flushes(mut self, flushes: usize) -> Self {
        self.max_concurrent_flushes = flushes;
        self
    }

    pub fn max_batch(&self) -> usize {
        self.max_batch
    }

    pub fn max_concurrent_flushes(&self) -> usize {
        self.max_concurrent_flushes
    }

    pub(crate) fn check(&self) -> Result<(), CheckpointStoreError> {
        if self.max_batch == 0 {
            return Err(CheckpointStoreError(
                "group commit's max batch must be at least 1".to_string(),
            ));
        }
        if self.max_concurrent_flushes == 0 {
            return Err(CheckpointStoreError(
                "group commit's max concurrent flushes must be at least 1".to_string(),
            ));
        }
        Ok(())
    }
}

/// Records the size of a checkpoint a store wrote, as `encoding`.
pub(crate) fn record_stored(store: &'static str, encoding: &'static str, bytes: usize) {
    metrics::histogram!(CHECKPOINT_SIZE_METRIC, STORE_LABEL => store, ENCODING_LABEL => encoding)
        .record(bytes as f64);
}

/// Records a row a store wrote, as `encoding` stores it, and the blobs it
/// wrote with it.
pub(crate) fn record_saved(store: &'static str, encoding: &crate::deltas::Encoding, bytes: usize) {
    record_stored(store, encoding.label(), bytes);
    for blob in &encoding.blobs {
        metrics::counter!(BLOBS_METRIC, STORE_LABEL => store).increment(1);
        metrics::histogram!(BLOB_SIZE_METRIC, STORE_LABEL => store)
            .record(blob.content.len() as f64);
    }
}

/// What became of one save in a flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Appended.
    Saved,
    /// A conditional save whose thread had moved past its expected sequence:
    /// nothing written.
    Lost,
    /// A fenced save whose lease token was no longer the thread's latest:
    /// nothing written.
    LeaseLost,
}

/// One save of a flush: the checkpoint, the sequence a conditional save
/// expects, and the lease token a fenced save is made under.
#[derive(Debug, Clone)]
pub(crate) struct Row {
    pub checkpoint: Checkpoint,
    pub expected: Option<i64>,
    pub token: Option<i64>,
}

/// A store's flush: every row in one transaction, each row's verdict in order.
/// It fails as a whole, having written nothing that counts; the queue then
/// fails the rows, or, when the failure isn't transient and there are several
/// rows, flushes each alone so only a row at fault fails.
pub(crate) type Flush =
    Arc<dyn Fn(Arc<Vec<Row>>) -> BoxFuture<'static, Result<Vec<Verdict>, Failure>> + Send + Sync>;

struct Entry {
    row: Row,
    arrived: Instant,
    answer: oneshot::Sender<Result<Verdict, CheckpointStoreError>>,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Entry>,
    /// Threads with a save in a running flush, and how many: their later saves
    /// wait for a later flush.
    in_flight: HashMap<String, usize>,
    flushing: usize,
}

struct Inner {
    max_batch: usize,
    max_flushes: usize,
    store: &'static str,
    flush: Flush,
    state: Mutex<State>,
}

/// The queue of a store with [`GroupCommit`]: takes saves, hands them to the
/// store's flush in batches, each on a task of its own, and gives each save its
/// answer. A save whose future is dropped while it waits is withdrawn; one a
/// flush has taken is written or not by that flush, whoever waits for it.
pub(crate) struct Queue {
    inner: Arc<Inner>,
}

impl Queue {
    /// A queue running up to `max_flushes` flushes at once, or as `options`
    /// say when `None`.
    pub fn new(
        options: &GroupCommit,
        store: &'static str,
        flush: Flush,
        max_flushes: Option<usize>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                max_batch: options.max_batch,
                max_flushes: max_flushes.unwrap_or(options.max_concurrent_flushes),
                store,
                flush,
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// Queues `row` and waits for its flush's verdict.
    pub async fn save(&self, row: Row) -> Result<Verdict, CheckpointStoreError> {
        let (answer, verdict) = oneshot::channel();
        let batches = {
            let mut state = self.inner.state.lock().map_err(|_| poisoned())?;
            state.queue.push_back(Entry {
                row,
                arrived: Instant::now(),
                answer,
            });
            self.inner.take_batches(&mut state)
        };
        for batch in batches {
            Inner::start(self.inner.clone(), batch);
        }
        verdict.await.map_err(|_| {
            CheckpointStoreError(format!(
                "the {} store's flush ended without answering a save",
                self.inner.store
            ))
        })?
    }
}

fn poisoned() -> CheckpointStoreError {
    CheckpointStoreError("group commit's queue is poisoned".to_string())
}

impl Inner {
    /// Under the lock: the batches to start now, while flushes may start. A
    /// batch takes queued saves in arrival order, up to the batch size,
    /// skipping every save of a thread already in a running flush; a save whose
    /// caller stopped waiting is dropped.
    fn take_batches(&self, state: &mut State) -> Vec<Vec<Entry>> {
        let mut batches = Vec::new();
        while state.flushing < self.max_flushes && !state.queue.is_empty() {
            let mut batch = Vec::new();
            let mut kept = VecDeque::with_capacity(state.queue.len());
            while let Some(entry) = state.queue.pop_front() {
                if entry.answer.is_closed() {
                    continue;
                }
                if batch.len() < self.max_batch
                    && !state
                        .in_flight
                        .contains_key(&entry.row.checkpoint.thread_id)
                {
                    batch.push(entry);
                } else {
                    kept.push_back(entry);
                }
            }
            state.queue = kept;
            if batch.is_empty() {
                break;
            }
            for entry in &batch {
                *state
                    .in_flight
                    .entry(entry.row.checkpoint.thread_id.clone())
                    .or_default() += 1;
            }
            state.flushing += 1;
            batches.push(batch);
        }
        batches
    }

    /// Runs `batch` on a task of its own, so a caller that stops waiting stops
    /// nobody else's save.
    fn start(inner: Arc<Inner>, batch: Vec<Entry>) {
        tokio::spawn(async move {
            // Gives the flush's place back and starts the next, however the
            // flush ends, a panic included.
            let threads: Vec<String> = batch
                .iter()
                .map(|entry| entry.row.checkpoint.thread_id.clone())
                .collect();
            let _done = Done {
                inner: inner.clone(),
                threads,
            };
            inner.run(batch).await;
        });
    }

    async fn run(&self, batch: Vec<Entry>) {
        metrics::histogram!(FLUSH_SIZE_METRIC, STORE_LABEL => self.store)
            .record(batch.len() as f64);
        let now = Instant::now();
        for entry in &batch {
            metrics::histogram!(FLUSH_WAIT_METRIC, STORE_LABEL => self.store)
                .record(now.duration_since(entry.arrived).as_secs_f64());
        }

        let (rows, answers): (Vec<Row>, Vec<_>) = batch
            .into_iter()
            .map(|entry| (entry.row, entry.answer))
            .unzip();
        let rows = Arc::new(rows);
        match (self.flush)(rows.clone()).await {
            Ok(verdicts) => {
                for (answer, verdict) in answers.into_iter().zip(verdicts) {
                    let _ = answer.send(Ok(verdict));
                }
            }
            Err(failure) if rows.len() > 1 && !failure.transient => {
                // One save's data may be what failed the flush: each saves
                // alone, so only that one fails.
                for (row, answer) in rows.iter().zip(answers) {
                    let alone = (self.flush)(Arc::new(vec![row.clone()])).await;
                    let _ = answer.send(
                        alone
                            .map(|verdicts| verdicts[0])
                            .map_err(CheckpointStoreError::from),
                    );
                }
            }
            Err(failure) => {
                for answer in answers {
                    let _ = answer.send(Err(failure.clone().into()));
                }
            }
        }
    }
}

/// A running flush's bookkeeping, done when it ends however it ends.
struct Done {
    inner: Arc<Inner>,
    threads: Vec<String>,
}

impl Drop for Done {
    fn drop(&mut self) {
        let batches = match self.inner.state.lock() {
            Ok(mut state) => {
                state.flushing -= 1;
                for thread in &self.threads {
                    if let Some(count) = state.in_flight.get_mut(thread) {
                        *count -= 1;
                        if *count == 0 {
                            state.in_flight.remove(thread);
                        }
                    }
                }
                self.inner.take_batches(&mut state)
            }
            Err(_) => Vec::new(),
        };
        for batch in batches {
            Inner::start(self.inner.clone(), batch);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalium_strata::schema::CheckpointStatus;
    use serde_json::json;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tokio::sync::Notify;

    fn row(thread: &str, n: u32) -> Row {
        Row {
            checkpoint: Checkpoint {
                schema_version: "1.0.0".to_string(),
                thread_id: thread.to_string(),
                status: CheckpointStatus::Interrupted,
                suspended_node: None,
                next_node: None,
                frozen_state: json!({ "n": n }),
                metadata: None,
                created_at: format!("2026-10-15T00:00:{n:02}Z"),
                resumed_at: None,
                nested: None,
                decision: None,
                wait: None,
                graph: None,
            },
            expected: None,
            token: None,
        }
    }

    fn name(row: &Row) -> String {
        format!(
            "{}@{}",
            row.checkpoint.thread_id,
            &row.checkpoint.created_at[17..19]
        )
    }

    /// A flush that records its batches, waits the first time a batch starts
    /// with a row of `hold` until released, fails a batch holding `bad`, and
    /// otherwise saves every row.
    struct Recorder {
        batches: StdMutex<Vec<Vec<String>>>,
        holding: std::sync::atomic::AtomicBool,
        held: Notify,
        release: Notify,
    }

    fn recorder(hold: &'static str, bad: &'static str) -> (Arc<Recorder>, Flush) {
        let recorder = Arc::new(Recorder {
            batches: StdMutex::new(Vec::new()),
            holding: std::sync::atomic::AtomicBool::new(true),
            held: Notify::new(),
            release: Notify::new(),
        });
        let flush_recorder = recorder.clone();
        let flush: Flush = Arc::new(move |rows: Arc<Vec<Row>>| {
            let recorder = flush_recorder.clone();
            Box::pin(async move {
                recorder
                    .batches
                    .lock()
                    .unwrap()
                    .push(rows.iter().map(name).collect());
                if rows[0].checkpoint.thread_id == hold
                    && recorder
                        .holding
                        .swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    recorder.held.notify_one();
                    recorder.release.notified().await;
                }
                if rows.iter().any(|row| row.checkpoint.thread_id == bad) {
                    return Err(Failure::fatal("a row the database refused"));
                }
                Ok(rows
                    .iter()
                    .map(|row| match row.checkpoint.thread_id.as_str() {
                        "lost" => Verdict::Lost,
                        "fenced" => Verdict::LeaseLost,
                        _ => Verdict::Saved,
                    })
                    .collect())
            })
        });
        (recorder, flush)
    }

    fn one_at_a_time() -> GroupCommit {
        GroupCommit::new().with_max_concurrent_flushes(1)
    }

    const PATIENCE: Duration = Duration::from_secs(30);

    #[tokio::test]
    async fn a_save_alone_is_flushed_at_once_and_alone() {
        let (recorder, flush) = recorder("none", "none");
        let queue = Queue::new(&one_at_a_time(), "test", flush, None);

        let verdict = tokio::time::timeout(PATIENCE, queue.save(row("a", 1)))
            .await
            .unwrap();

        assert_eq!(verdict, Ok(Verdict::Saved));
        assert_eq!(*recorder.batches.lock().unwrap(), vec![vec!["a@01"]]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saves_that_arrive_during_a_flush_go_together_up_to_the_batch_size() {
        let (recorder, flush) = recorder("first", "none");
        let queue = Arc::new(Queue::new(
            &one_at_a_time().with_max_batch(3),
            "test",
            flush,
            None,
        ));

        let blocker = tokio::spawn({
            let queue = queue.clone();
            async move { queue.save(row("first", 0)).await }
        });
        recorder.held.notified().await;
        let mut waiting = Vec::new();
        for i in 1..=5 {
            let queue = queue.clone();
            waiting.push(tokio::spawn(async move {
                queue.save(row(&format!("t{i}"), i)).await
            }));
            // Queued in this order.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        recorder.release.notify_one();
        tokio::time::timeout(PATIENCE, blocker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        for save in waiting {
            tokio::time::timeout(PATIENCE, save)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }

        assert_eq!(
            *recorder.batches.lock().unwrap(),
            vec![
                vec!["first@00"],
                vec!["t1@01", "t2@02", "t3@03"],
                vec!["t4@04", "t5@05"],
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_threads_saves_keep_their_order_and_wait_for_its_running_flush() {
        let (recorder, flush) = recorder("t", "none");
        let queue = Arc::new(Queue::new(
            &GroupCommit::new().with_max_concurrent_flushes(2),
            "test",
            flush,
            None,
        ));

        let blocker = tokio::spawn({
            let queue = queue.clone();
            async move { queue.save(row("t", 0)).await }
        });
        recorder.held.notified().await;
        let later = tokio::spawn({
            let queue = queue.clone();
            async move { queue.save(row("t", 1)).await }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        // A second flush may run, but not with t's next saves.
        tokio::time::timeout(PATIENCE, queue.save(row("u", 1)))
            .await
            .unwrap()
            .unwrap();
        let last = tokio::spawn({
            let queue = queue.clone();
            async move { queue.save(row("t", 2)).await }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!later.is_finished());
        recorder.release.notify_one();
        for save in [blocker, later, last] {
            tokio::time::timeout(PATIENCE, save)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }

        let batches = recorder.batches.lock().unwrap().clone();
        assert_eq!(batches[0], vec!["t@00"]);
        assert_eq!(batches[1], vec!["u@01"]);
        // t's later saves go together once its first flush has ended. The
        // first of them may be flushed alone if it is let go before the last
        // one arrives: either way, in order.
        let rest: Vec<String> = batches[2..].iter().flatten().cloned().collect();
        assert_eq!(rest, vec!["t@01", "t@02"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_flush_that_fails_for_good_is_tried_again_save_by_save() {
        let (recorder, flush) = recorder("first", "bad");
        let queue = Arc::new(Queue::new(&one_at_a_time(), "test", flush, None));

        let blocker = tokio::spawn({
            let queue = queue.clone();
            async move { queue.save(row("first", 0)).await }
        });
        recorder.held.notified().await;
        let mut saves = Vec::new();
        for (i, thread) in ["good", "bad", "also"].into_iter().enumerate() {
            let queue = queue.clone();
            saves.push(tokio::spawn(async move {
                queue.save(row(thread, i as u32 + 1)).await
            }));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        recorder.release.notify_one();
        tokio::time::timeout(PATIENCE, blocker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut results = Vec::new();
        for save in saves {
            results.push(tokio::time::timeout(PATIENCE, save).await.unwrap().unwrap());
        }

        assert_eq!(results[0], Ok(Verdict::Saved));
        assert_eq!(
            results[1],
            Err(CheckpointStoreError(
                "a row the database refused".to_string()
            ))
        );
        assert_eq!(results[2], Ok(Verdict::Saved));
        assert_eq!(
            *recorder.batches.lock().unwrap(),
            vec![
                vec!["first@00"],
                vec!["good@01", "bad@02", "also@03"],
                vec!["good@01"],
                vec!["bad@02"],
                vec!["also@03"],
            ]
        );
    }

    #[tokio::test]
    async fn a_transient_failure_fails_every_save_of_the_flush() {
        let flush: Flush =
            Arc::new(|_| Box::pin(async { Err(Failure::injected("save:before-commit")) }));
        let queue = Queue::new(&one_at_a_time(), "test", flush, None);

        let failed = queue.save(row("a", 1)).await;

        assert_eq!(
            failed,
            Err(CheckpointStoreError(
                "injected fault at save:before-commit".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn a_verdict_reaches_its_own_caller_only() {
        let (_, flush) = recorder("none", "none");
        let queue = Queue::new(&one_at_a_time(), "test", flush, None);

        assert_eq!(queue.save(row("lost", 1)).await, Ok(Verdict::Lost));
        assert_eq!(queue.save(row("fenced", 1)).await, Ok(Verdict::LeaseLost));
        assert_eq!(queue.save(row("fine", 1)).await, Ok(Verdict::Saved));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_queued_save_whose_future_is_dropped_is_withdrawn() {
        let (recorder, flush) = recorder("first", "none");
        let queue = Arc::new(Queue::new(&one_at_a_time(), "test", flush, None));

        let blocker = tokio::spawn({
            let queue = queue.clone();
            async move { queue.save(row("first", 0)).await }
        });
        recorder.held.notified().await;
        let withdrawn = tokio::spawn({
            let queue = queue.clone();
            async move { queue.save(row("gone", 1)).await }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        withdrawn.abort();
        let _ = withdrawn.await;
        let kept = tokio::spawn({
            let queue = queue.clone();
            async move { queue.save(row("kept", 2)).await }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        recorder.release.notify_one();
        tokio::time::timeout(PATIENCE, blocker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(PATIENCE, kept)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(
            *recorder.batches.lock().unwrap(),
            vec![vec!["first@00"], vec!["kept@02"]]
        );
    }

    #[test]
    fn the_options_are_checked() {
        assert!(GroupCommit::new().with_max_batch(0).check().is_err());
        assert!(
            GroupCommit::new()
                .with_max_concurrent_flushes(0)
                .check()
                .is_err()
        );
        let defaults = GroupCommit::new();
        assert_eq!(
            (defaults.max_batch(), defaults.max_concurrent_flushes()),
            (256, 4)
        );
        assert!(defaults.check().is_ok());
    }
}
