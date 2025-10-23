use crate::activity::activity::{ActivityFuture, ActivityHandlerRegistry, ActivityOption};
use crate::config::WorkerConfig;
use crate::queue::queue::{ActivityQueueTrait, ActivityResult, ResultState};
use crate::runner::error::WorkerError;
use crate::{
    activity::activity::Activity, ActivityContext, ActivityError, ActivityHandler, ActivityQueue,
};
use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use tokio_util::sync::CancellationToken;

/// Optional metrics sink to expose counters without coupling to a specific backend.
pub trait MetricsSink: Send + Sync + 'static {
    fn inc_counter(&self, name: &str, value: u64);
    fn observe_duration(&self, _name: &str, _dur: Duration) {
        let _ = (_name, _dur);
    }
}

/// No-op metrics sink
pub struct NoopMetrics;
impl MetricsSink for NoopMetrics {
    fn inc_counter(&self, _name: &str, _value: u64) {}
}

/// Simple exponential backoff helper for idle polls
struct Backoff {
    current: Duration,
    base: Duration,
    max: Duration,
}
impl Backoff {
    fn new(base: Duration, max: Duration) -> Self {
        Self {
            current: base,
            base,
            max,
        }
    }
    fn reset(&mut self) {
        self.current = self.base;
    }
    fn next(&mut self) -> Duration {
        let next = self.current;
        self.current = (self.current.mul_f32(2.0)).min(self.max);
        next
    }
}

pub struct WorkerEngine {
    activity_queue: Arc<dyn ActivityQueueTrait>,
    activity_handlers: ActivityHandlerRegistry, // kept as-is per request
    config: WorkerConfig,
    running: Arc<RwLock<bool>>, // retains external visibility
    shutdown_tx: watch::Sender<bool>,
    cancel_token: CancellationToken,
    metrics: Arc<dyn MetricsSink>,
}

impl WorkerEngine {
    pub fn new(redis_pool: Pool<RedisConnectionManager>, config: WorkerConfig) -> Self {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        Self {
            activity_queue: Arc::new(ActivityQueue::new(redis_pool, config.queue_name.clone())),
            activity_handlers: ActivityHandlerRegistry::new(),
            config,
            running: Arc::new(RwLock::new(false)),
            shutdown_tx,
            cancel_token: CancellationToken::new(),
            metrics: Arc::new(NoopMetrics),
        }
    }

    /// Optionally plug in a metrics sink.
    pub fn with_metrics(mut self, sink: Arc<dyn MetricsSink>) -> Self {
        self.metrics = sink;
        self
    }

    /// Starts the worker engine and manages its full execution lifecycle.
    ///
    /// This method performs the following steps:
    /// - Verifies that the engine is not already running, returning `WorkerError::AlreadyRunning` if it is.
    /// - Marks the engine as active and initializes a semaphore to cap concurrent activity execution
    ///   at `config.max_concurrent_activities`.
    /// - Spawns both:
    ///   - a background task that periodically processes scheduled activities, and
    ///   - a pool of worker loops (one per available concurrency slot) that dequeue and execute activities.
    /// - Awaits either:
    ///   - a shutdown signal (Ctrl+C or SIGTERM), or
    ///   - the completion or failure of any worker loop.
    /// - Once a shutdown signal is received, transitions the engine into graceful stop mode,
    ///   halting new activity execution and allowing in-flight work to complete before cleanup.
    ///
    /// # Behavior
    ///
    /// - The method runs until the engine is explicitly stopped or a shutdown signal is received.
    /// - When it returns, the engine is fully stopped and resources have been released.
    /// - If the engine was already running, it immediately returns `Err(WorkerError::AlreadyRunning)`.
    ///
    /// # Returns
    ///
    /// - `Ok(())` — the engine shut down cleanly.
    /// - `Err(WorkerError::AlreadyRunning)` — start was attempted while another instance was active.
    /// - Other `WorkerError` variants — internal initialization or runtime failures.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example() -> Result<(), WorkerError> {
    /// // Initialize the engine (pseudo-code)
    /// let engine = WorkerEngine::new(redis_pool, config);
    ///
    /// // Start processing activities — this call will block until shutdown
    /// engine.start().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start(&self) -> Result<(), WorkerError> {
        {
            let mut running = self.running.write().await;
            if *running {
                return Err(WorkerError::AlreadyRunning);
            }
            *running = true;
        }

        info!(
            max_concurrent_activities = self.config.max_concurrent_activities,
            "Starting worker engine"
        );

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_activities));
        let mut join_handles = Vec::new();

        // Scheduled activities processor
        let scheduled_handle = self.start_scheduled_activities_processor().await;
        join_handles.push(scheduled_handle);

        // Worker loops (now return Result)
        for worker_id in 0..self.config.max_concurrent_activities {
            let handle = self.start_worker_loop(worker_id, semaphore.clone()).await;
            join_handles.push(handle);
        }

        // Wait for shutdown signal or all workers finishing.
        tokio::select! {
            _ = self.wait_for_shutdown() => {
                info!("Shutdown signal received, stopping worker engine");
            }
            result = futures::future::try_join_all(join_handles) => {
                match result {
                    Ok(_) => info!("All worker loops completed"),
                    Err(e) => error!(error = %e, "A worker task failed"),
                }
            }
        }

        self.stop().await;
        info!("Worker engine stopped");
        Ok(())
    }

    /// Signal a graceful stop.
    pub async fn stop(&self) {
        info!("Stopping worker engine");
        let mut running = self.running.write().await;
        *running = false;
        // broadcast shutdown
        let _ = self.shutdown_tx.send(true);
        self.cancel_token.cancel();
    }

    /// Spawns a background worker loop that continuously dequeues and executes activities.
    ///
    /// Each worker operates independently and performs the following cycle while the engine is running:
    /// - Acquires a permit from the provided semaphore to enforce the global concurrency limit.
    /// - Attempts to dequeue an activity from the queue (waiting briefly if none are available).
    /// - Looks up the registered handler for the dequeued activity type.
    /// - Executes the handler with a per-activity timeout, passing in an `ActivityContext` containing
    ///   metadata and a reference back to the engine for nested execution.
    /// - Records the outcome through the activity queue:
    ///   - **Success:** Marks the activity as completed and stores the result with `ResultState::Ok`.
    ///   - **Retryable failure (`ActivityError::Retry`)**: Marks the activity as failed and eligible for retry.
    ///   - **Non-retryable failure (`ActivityError::NonRetry`)**: Marks the activity as permanently failed and
    ///     stores a structured JSON error (`{"error": <reason>, "type": "non_retryable", "failed_at": <RFC3339>}`).
    ///   - **Timeout:** If execution exceeds its allowed duration, the activity is marked as failed and retried.
    ///
    /// The worker loop terminates when:
    /// - The engine’s running flag is cleared (via `stop()`), or
    /// - A shutdown signal is received through cooperative cancellation.
    ///
    /// Upon termination, any ongoing work completes its current iteration before the worker exits.
    ///
    /// # Returns
    ///
    /// Returns a [`tokio::task::JoinHandle`] wrapping the background worker task.
    /// The handle can be awaited to monitor completion or detached to run in the background.
    ///
    /// # Notes
    ///
    /// - This function does not block; it spawns a background task and immediately returns.
    /// - Each worker is lightweight and safe to run concurrently; the semaphore ensures that
    ///   the total number of active activities never exceeds the configured concurrency limit.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use tokio::sync::Semaphore;
    /// # async fn example(engine: &crate::runner::WorkerEngine) {
    /// let semaphore = Arc::new(Semaphore::new(4));
    ///
    /// // Spawn a worker loop with ID 0
    /// let handle = engine.start_worker_loop(0, semaphore.clone()).await;
    ///
    /// // The returned JoinHandle runs until the engine stops or a shutdown signal is received.
    /// handle.await.ok();
    /// # }
    /// ```
    async fn start_worker_loop(
        &self,
        worker_id: usize,
        semaphore: Arc<Semaphore>,
    ) -> tokio::task::JoinHandle<Result<(), WorkerError>> {
        let running = self.running.clone();
        let activity_queue = self.activity_queue.clone();
        let activity_handlers = self.activity_handlers.clone();
        let activity_queue_for_context = self.activity_queue.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let cancel_token = self.cancel_token.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            debug!(%worker_id, "Starting worker loop");
            let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));

            while *running.read().await {
                // Honor fast shutdown
                if *shutdown_rx.borrow() {
                    break;
                }

                // Acquire permit (bounded concurrency)
                let permit = match semaphore.try_acquire() {
                    Ok(p) => p,
                    Err(_) => {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                            _ = shutdown_rx.changed() => break,
                        }
                        continue;
                    }
                };

                // Dequeue with cooperative shutdown
                let dequeue_fut = activity_queue.dequeue(Duration::from_secs(1));
                let activity_opt = tokio::select! {
                    _ = shutdown_rx.changed() => { drop(permit); break; }
                    res = dequeue_fut => res
                };

                let activity = match activity_opt {
                    Ok(Some(a)) => {
                        backoff.reset();
                        a
                    }
                    Ok(None) => {
                        drop(permit);
                        let sleep_for = backoff.next();
                        tokio::select! {
                            _ = tokio::time::sleep(sleep_for) => {},
                            _ = shutdown_rx.changed() => break,
                        }
                        continue;
                    }
                    Err(e) => {
                        error!(%worker_id, error = %e, "Failed to dequeue activity");
                        drop(permit);
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                            _ = shutdown_rx.changed() => break,
                        }
                        continue;
                    }
                };

                // Resolve handler
                let activity_id = activity.id;
                let activity_type = activity.activity_type.clone();
                debug!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, "Worker processing activity");

                let handler = match activity_handlers.get(&activity.activity_type) {
                    Some(h) => h.clone(),
                    None => {
                        error!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, "No handler found for activity type");
                        if let Err(e) = activity_queue
                            .mark_failed(activity, "handler_not_found".to_string(), false)
                            .await
                        {
                            error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to mark activity as failed");
                        }
                        drop(permit);
                        continue;
                    }
                };

                // Prepare context
                let context = ActivityContext {
                    activity_id,
                    activity_type: activity_type.clone(),
                    retry_count: activity.retry_count,
                    metadata: activity.metadata.clone(),
                    cancel_token: cancel_token.child_token(),
                    worker_engine: Arc::new(WorkerEngineWrapper {
                        activity_queue: activity_queue_for_context.clone(),
                    }),
                };

                let activity_timeout = Duration::from_secs(activity.timeout_seconds);
                let handle_fut = handler.handle(activity.payload.clone(), context);

                // Execute with timeout and cooperative shutdown
                let timed = tokio::select! {
                    _ = shutdown_rx.changed() => {
                        drop(permit);
                        break;
                    }
                    res = tokio::time::timeout(activity_timeout, handle_fut) => res
                };

                match timed {
                    Ok(Ok(value)) => {
                        metrics.inc_counter("activity_completed", 1);
                        if let Err(e) = activity_queue.mark_completed(activity.id).await {
                            error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to mark activity as completed");
                        }
                        info!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, "Activity completed successfully");

                        // Store result (fire-and-forget to avoid blocking the worker on slow I/O)
                        let aq = activity_queue.clone();
                        let result_to_store = ActivityResult {
                            data: value,
                            state: ResultState::Ok,
                        };
                        tokio::spawn(async move {
                            if let Err(e) = aq.store_result(activity_id, result_to_store).await {
                                error!(activity_id = %activity_id, error = %e, "Failed to store activity result");
                            }
                        });
                    }
                    Ok(Err(ActivityError::Retry(reason))) => {
                        metrics.inc_counter("activity_retry", 1);
                        warn!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, reason = %reason, "Activity requesting retry");
                        if let Err(e) = activity_queue.mark_failed(activity, reason, true).await {
                            error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to mark activity for retry");
                        }
                    }
                    Ok(Err(ActivityError::NonRetry(reason))) => {
                        metrics.inc_counter("activity_failed_non_retry", 1);
                        error!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, reason = %reason, "Activity failed");
                        if let Err(e) = activity_queue
                            .mark_failed(activity, reason.clone(), false)
                            .await
                        {
                            error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to mark activity as failed");
                        }
                        let aq = activity_queue.clone();
                        tokio::spawn(async move {
                            let activity_result = ActivityResult {
                                data: Some(json!({
                                    "error": reason,
                                    "type": "non_retryable",
                                    "failed_at": chrono::Utc::now().to_rfc3339()
                                })),
                                state: ResultState::Err,
                            };
                            if let Err(e) = aq.store_result(activity_id, activity_result).await {
                                error!(activity_id = %activity_id, error = %e, "Failed to store activity result");
                            }
                        });
                    }
                    Err(_elapsed) => {
                        metrics.inc_counter("activity_timeout", 1);
                        let error_msg = "Activity execution timed out".to_string();
                        error!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, timeout = ?activity_timeout, "Activity timed out");
                        if let Err(e) = activity_queue.mark_failed(activity, error_msg, true).await
                        {
                            error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to mark activity as failed");
                        }
                    }
                }

                drop(permit);
            }

            debug!(%worker_id, "Worker loop stopped");
            Ok(())
        })
    }

    /// Spawns a background processor that periodically executes scheduled activities.
    ///
    /// This task runs continuously while the engine remains active, performing the following loop:
    /// - Calls [`process_scheduled_activities()`] on the engine’s activity queue to identify and enqueue
    ///   any activities whose scheduled execution time has arrived.
    /// - Logs any errors encountered during processing but continues operation.
    /// - Waits for a fixed interval (default: 30 seconds, configurable via `config.schedule_poll_interval_seconds`)
    ///   before repeating the cycle.
    ///
    /// The processor automatically stops when:
    /// - The engine’s running flag is cleared (via [`stop()`]), or
    /// - A shutdown signal is received through cooperative cancellation.
    ///
    /// This mechanism ensures that time-delayed or recurring activities are regularly promoted
    /// into the execution queue for handling by worker loops.
    ///
    /// # Returns
    ///
    /// Returns a [`tokio::task::JoinHandle`] representing the spawned background processor.
    /// The handle can be:
    /// - **awaited**, to wait for graceful completion; or
    /// - **aborted**, to terminate the processor immediately.
    ///
    /// # Notes
    ///
    /// - Failures during scheduled-activity processing are logged and do not halt the engine.
    /// - The polling interval can be tuned through the worker configuration to balance responsiveness and load.
    ///
    /// # Examples
    ///
    /// ```
    /// // Assuming `engine` is an initialized WorkerEngine instance:
    /// let handle = engine.start_scheduled_activities_processor().await;
    ///
    /// // The processor runs in the background until the engine stops.
    /// // You may choose to abort or await it as needed:
    /// handle.abort();
    /// // or
    /// handle.await.ok();
    /// ```
    async fn start_scheduled_activities_processor(
        &self,
    ) -> tokio::task::JoinHandle<Result<(), WorkerError>> {
        let activity_queue = self.activity_queue.clone();
        let running = self.running.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Make poll interval configurable; default to 30s if config lacks it.
        let poll_interval = {
            let secs = self
                .config
                .schedule_poll_interval_seconds
                .unwrap_or(30)
                .max(1);
            Duration::from_secs(secs)
        };

        tokio::spawn(async move {
            debug!("Starting scheduled activities processor");
            while *running.read().await {
                if *shutdown_rx.borrow() {
                    break;
                }

                if let Err(e) = activity_queue.process_scheduled_activities().await {
                    error!(error = %e, "Failed to process scheduled activities");
                }

                tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => {},
                    _ = shutdown_rx.changed() => break,
                }
            }
            debug!("Scheduled activities processor stopped");
            Ok(())
        })
    }

    async fn wait_for_shutdown(&self) {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            sigterm.recv().await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => { info!("Received Ctrl+C signal"); },
            _ = terminate => { info!("Received SIGTERM signal"); },
        }

        self.stop().await;
    }
}

impl WorkerEngine {
    pub async fn execute_activity(
        &self,
        activity_type: String,
        payload: serde_json::Value,
        option: Option<ActivityOption>,
    ) -> Result<ActivityFuture, WorkerError> {
        let activity = Activity::new(activity_type, payload, option);
        let activity_id = activity.id;
        match activity.scheduled_at {
            None => self.activity_queue.enqueue(activity).await?,
            Some(_) => self.activity_queue.schedule_activity(activity).await?,
        }
        Ok(ActivityFuture::new(
            self.activity_queue.clone(),
            activity_id,
        ))
    }

    pub fn register_activity(&mut self, activity_type: String, activity: Arc<dyn ActivityHandler>) {
        self.activity_handlers.insert(activity_type, activity);
    }

    pub fn get_activity_executor(&self) -> Arc<dyn ActivityExecutor> {
        Arc::new(WorkerEngineWrapper {
            activity_queue: self.activity_queue.clone(),
        })
    }
}

#[async_trait::async_trait]
pub trait ActivityExecutor: Send + Sync {
    async fn execute_activity(
        &self,
        activity_type: String,
        payload: serde_json::Value,
        option: Option<ActivityOption>,
    ) -> Result<ActivityFuture, WorkerError>;
}

#[derive(Clone)]
pub struct WorkerEngineWrapper {
    activity_queue: Arc<dyn ActivityQueueTrait>,
}

#[async_trait::async_trait]
impl ActivityExecutor for WorkerEngineWrapper {
    async fn execute_activity(
        &self,
        activity_type: String,
        payload: serde_json::Value,
        option: Option<ActivityOption>,
    ) -> Result<ActivityFuture, WorkerError> {
        let activity = Activity::new(activity_type, payload, option);
        let activity_id = activity.id;
        match activity.scheduled_at {
            None => self.activity_queue.enqueue(activity).await?,
            Some(_) => self.activity_queue.schedule_activity(activity).await?,
        }
        Ok(ActivityFuture::new(
            self.activity_queue.clone(),
            activity_id,
        ))
    }
}
