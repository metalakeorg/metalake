//! Footers kept after a commit. A file an Iceberg client adds comes with no footer size, so the commit reads none of
//! them (STA-1); once it has answered, one worker reads each such file's tail and keeps its footer (FTR-1), so that a
//! read plans its row groups from the catalog within seconds. Off the path of every request: the queue is bounded, and
//! a commit that finds it full goes on, its files left to maintenance (`metalake maintain footers`), as is whatever a
//! worker fails at. Stopping lets what is queued finish, for a while.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Commits whose files wait for their footers at most.
pub(crate) const QUEUE: usize = 64;

/// The files of one table that one commit added with no footer: each a path and its size.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Job {
    pub table_id: Uuid,
    pub files: Vec<(String, u64)>,
}

enum Message {
    Keep(Job),
    Stop,
}

/// The queue of a worker that keeps footers; a clone sends to the same worker.
#[derive(Debug, Clone)]
pub(crate) struct Footers {
    queue: mpsc::Sender<Message>,
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Footers {
    /// A worker that runs `keep` on each job in turn, with room for `capacity` jobs waiting.
    pub(crate) fn start<F, Fut>(capacity: usize, keep: F) -> Self
    where
        F: Fn(Job) -> Fut + Send + 'static,
        Fut: Future<Output = Result<u64, String>> + Send,
    {
        let (queue, mut jobs) = mpsc::channel(capacity);
        let worker = tokio::spawn(async move {
            while let Some(Message::Keep(job)) = jobs.recv().await {
                let (table, files) = (job.table_id, job.files.len());
                if let Err(e) = keep(job).await {
                    eprintln!("metalake-iceberg: the footers of {files} files of table {table} are left: {e}");
                }
            }
        });
        Self {
            queue,
            worker: Arc::new(Mutex::new(Some(worker))),
        }
    }

    /// Queues a commit's files, and answers whether they were queued: a full queue leaves them to maintenance, and the
    /// commit never waits for it.
    pub(crate) fn queue(&self, job: Job) -> bool {
        if job.files.is_empty() {
            return true;
        }
        match self.queue.try_send(Message::Keep(job)) {
            Ok(()) => true,
            Err(TrySendError::Full(Message::Keep(job)) | TrySendError::Closed(Message::Keep(job))) => {
                eprintln!(
                    "metalake-iceberg: {} files of table {} are left to maintenance, as the queue of footers is full",
                    job.files.len(),
                    job.table_id
                );
                false
            }
            Err(_) => false,
        }
    }

    /// Lets the jobs queued finish, within `within`, and stops the worker: what is left goes to maintenance.
    pub(crate) async fn stop(&self, within: Duration) {
        let worker = self.worker.lock().expect("not poisoned").take();
        let Some(worker) = worker else {
            return;
        };
        let drained = tokio::time::timeout(within, async {
            if self.queue.send(Message::Stop).await.is_ok() {
                let _ = worker.await;
            }
        })
        .await;
        if drained.is_err() {
            eprintln!("metalake-iceberg: stopped with footers still queued, left to maintenance");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn job(n: usize) -> Job {
        Job {
            table_id: Uuid::nil(),
            files: (0..n).map(|i| (format!("memory:///t/{i}"), 100)).collect(),
        }
    }

    #[tokio::test]
    async fn footers_queue_runs_jobs_in_order_then_stops() {
        let done = Arc::new(StdMutex::new(Vec::new()));
        let seen = done.clone();
        let footers = Footers::start(QUEUE, move |job: Job| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(job.files.len());
                Ok(job.files.len() as u64)
            }
        });
        assert!(footers.queue(job(1)));
        assert!(footers.queue(job(2)));
        assert!(footers.queue(job(0)), "nothing to keep is no job");
        footers.stop(Duration::from_secs(5)).await;
        assert_eq!(
            *done.lock().unwrap(),
            [1, 2],
            "what was queued finished before the stop"
        );
        assert!(!footers.queue(job(3)), "a stopped queue takes nothing");
    }

    #[tokio::test]
    async fn footers_queue_full_skips() {
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let wait = Arc::new(tokio::sync::Mutex::new(Some(wait)));
        let footers = Footers::start(1, move |_| {
            let wait = wait.clone();
            async move {
                if let Some(wait) = wait.lock().await.take() {
                    let _ = wait.await;
                }
                Ok(0)
            }
        });
        // the worker holds the first, the queue the second; the third does not wait
        assert!(footers.queue(job(1)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(footers.queue(job(1)));
        let started = std::time::Instant::now();
        assert!(!footers.queue(job(1)), "a full queue leaves it to maintenance");
        assert!(started.elapsed() < Duration::from_millis(50));
        let _ = release.send(());
        footers.stop(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn footers_stop_bounded_by_time() {
        let footers = Footers::start(QUEUE, |_| async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(0)
        });
        assert!(footers.queue(job(1)));
        let started = std::time::Instant::now();
        footers.stop(Duration::from_millis(100)).await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a stuck job does not hold the stop"
        );
    }

    #[tokio::test]
    async fn footers_failed_job_skips() {
        let footers = Footers::start(QUEUE, |_| async { Err("the store is down".to_string()) });
        assert!(footers.queue(job(1)));
        footers.stop(Duration::from_secs(5)).await;
    }
}
