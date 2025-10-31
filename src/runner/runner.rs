use crate::activity::activity::{
    ActivityFuture, ActivityHandlerRegistry, ActivityOption, ActivityPriority,
};
use crate::config::WorkerConfig;
use crate::queue::queue::{ActivityQueueTrait, ActivityResult, ResultState};
use crate::runner::error::WorkerError;
use crate::runner::redis::{create_redis_pool, create_redis_pool_with_config, RedisConfig};
use crate::{
    activity::activity::Activity, ActivityContext, ActivityError, ActivityHandler, ActivityQueue,
};
use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Optional metrics sink to expose counters without coupling to a specific backend.
///
/// This trait allows you to collect metrics about activity processing, including
/// completion rates, retry counts, and execution times. Implement this trait
/// to integrate with your preferred metrics system (Prometheus, StatsD, etc.).
///
/// # Examples
///
/// ```rust,no_run
/// use runner_q::MetricsSink;
/// use std::time::Duration;
/// use std::sync::Arc;
///
/// // Prometheus metrics implementation
/// struct PrometheusMetrics {
///     // Contains pre-registered Prometheus metrics
/// }
///
/// impl MetricsSink for PrometheusMetrics {
///     fn inc_counter(&self, name: &str, value: u64) {
///         // Increment the appropriate counter based on name
///     }
///     
///     fn observe_duration(&self, name: &str, duration: Duration) {
///         // Record the duration in the appropriate histogram
///     }
/// }
///
/// // Simple logging metrics implementation
/// struct LoggingMetrics;
///
/// impl MetricsSink for LoggingMetrics {
///     fn inc_counter(&self, name: &str, value: u64) {
///         println!("METRIC: {} += {}", name, value);
///     }
///     
///     fn observe_duration(&self, name: &str, duration: Duration) {
///         println!("METRIC: {} = {:?}", name, duration);
///     }
/// }
/// ```
pub trait MetricsSink: Send + Sync + 'static {
    /// Increment a counter metric by the specified value.
    ///
    /// This is typically used for counting events like activity completions,
    /// retries, failures, etc.
    fn inc_counter(&self, name: &str, value: u64);

    /// Record a duration metric.
    ///
    /// This is typically used for measuring execution times, queue wait times, etc.
    /// The default implementation is a no-op, so you only need to implement this
    /// if you want to collect duration metrics.
    fn observe_duration(&self, _name: &str, _dur: Duration) {
        let _ = (_name, _dur);
    }
}

/// No-op metrics sink that discards all metrics.
///
/// This is the default metrics implementation that does nothing with the metrics.
/// Use this when you don't need metrics collection or as a fallback.
///
/// # Examples
///
/// ```rust
/// use runner_q::{NoopMetrics, MetricsSink};
/// use std::time::Duration;
///
/// let metrics = NoopMetrics;
///
/// // These calls do nothing
/// metrics.inc_counter("activities_completed", 1);
/// metrics.observe_duration("activity_execution", Duration::from_secs(5));
/// ```
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
    /// Creates a new WorkerEngine with the given Redis connection pool and configuration.
    ///
    /// The engine is created in a stopped state and must be started with [`start()`] to begin
    /// processing activities. Before starting, you should register activity handlers using
    /// [`register_activity()`].
    ///
    /// # Parameters
    ///
    /// * `redis_pool` - Redis connection pool for queue operations
    /// * `config` - Configuration settings for the worker engine
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use runner_q::{WorkerEngine, WorkerConfig};
    /// use bb8_redis::bb8::Pool;
    /// use bb8_redis::RedisConnectionManager;
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Create Redis connection pool
    /// let redis_pool = Pool::builder()
    ///     .build(RedisConnectionManager::new("redis://127.0.0.1:6379")?)
    ///     .await?;
    ///
    /// // Create configuration
    /// let config = WorkerConfig {
    ///     queue_name: "my_app".to_string(),
    ///     max_concurrent_activities: 10,
    ///     redis_url: "redis://127.0.0.1:6379".to_string(),
    ///     schedule_poll_interval_seconds: Some(30),
    /// };
    ///
    /// // Create worker engine
    /// let mut worker_engine = WorkerEngine::new(redis_pool, config);
    ///
    /// // Register activity handlers before starting
    /// // worker_engine.register_activity("send_email".to_string(), Arc::new(EmailHandler));
    ///
    /// // Start the engine (this will block until shutdown)
    /// // worker_engine.start().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// For a more ergonomic API, consider using the builder pattern:
    ///
    /// ```rust,no_run
    /// use runner_q::WorkerEngine;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let engine = WorkerEngine::builder()
    ///     .redis_url("redis://localhost:6379")
    ///     .queue_name("my_app")
    ///     .max_workers(10)
    ///     .schedule_poll_interval(Duration::from_secs(30))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(redis_pool: Pool<RedisConnectionManager>, config: WorkerConfig) -> Self {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        Self {
            activity_queue: Arc::new(ActivityQueue::new(
                redis_pool,
                config.queue_name.clone(),
                config.lease_ms.unwrap_or(60_000),
            )),
            activity_handlers: ActivityHandlerRegistry::new(),
            config,
            running: Arc::new(RwLock::new(false)),
            shutdown_tx,
            cancel_token: CancellationToken::new(),
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_metrics(&mut self, sink: Arc<dyn MetricsSink>) {
        self.metrics = sink;
    }

    /// Creates a new WorkerEngineBuilder for fluent configuration.
    ///
    /// This provides a more ergonomic API for configuring the WorkerEngine with method chaining.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use runner_q::WorkerEngine;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let engine = WorkerEngine::builder()
    ///     .redis_url("redis://localhost:6379")
    ///     .queue_name("my_app")
    ///     .max_workers(8)
    ///     .schedule_poll_interval(Duration::from_secs(30))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    ///
    /// // With custom Redis configuration and metrics
    /// # async fn example_with_redis_config() -> Result<(), Box<dyn std::error::Error>> {
    /// use runner_q::{RedisConfig, MetricsSink};
    /// use std::sync::Arc;
    ///
    /// let redis_config = RedisConfig {
    ///     max_size: 100,
    ///     min_idle: 10,
    ///     conn_timeout: Duration::from_secs(60),
    ///     idle_timeout: Duration::from_secs(600),
    ///     max_lifetime: Duration::from_secs(3600),
    /// };
    ///
    /// // Custom metrics implementation
    /// struct PrometheusMetrics;
    /// impl MetricsSink for PrometheusMetrics {
    ///     fn inc_counter(&self, name: &str, value: u64) {
    ///         println!("Counter {}: {}", name, value);
    ///     }
    ///     fn observe_duration(&self, name: &str, duration: Duration) {
    ///         println!("Duration {}: {:?}", name, duration);
    ///     }
    /// }
    ///
    /// let engine = WorkerEngine::builder()
    ///     .redis_url("redis://localhost:6379")
    ///     .queue_name("my_app")
    ///     .max_workers(8)
    ///     .redis_config(redis_config)
    ///     .metrics(Arc::new(PrometheusMetrics))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> WorkerEngineBuilder {
        WorkerEngineBuilder::new()
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

        // Reaper processor for re-queueing expired processing items
        let reaper_handle = self.start_reaper_processor().await;
        join_handles.push(reaper_handle);

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

    /// Signal a graceful stop of the worker engine.
    ///
    /// This method initiates a graceful shutdown process:
    /// - Stops accepting new activities
    /// - Allows currently running activities to complete
    /// - Cancels any pending operations
    /// - Releases resources
    ///
    /// The shutdown is cooperative - activities should check the cancellation token
    /// and exit cleanly when requested.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use runner_q::{WorkerEngine, WorkerConfig};
    /// use std::sync::Arc;
    /// use tokio::time::{sleep, Duration};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut engine = WorkerEngine::new(redis_pool, config);
    ///
    /// // Start the engine in a background task
    /// let engine_handle = tokio::spawn(async move {
    ///     engine.start().await
    /// });
    ///
    /// // Let it run for a while
    /// sleep(Duration::from_secs(10)).await;
    ///
    /// // Gracefully stop the engine
    /// engine.stop().await;
    ///
    /// // Wait for the engine to finish
    /// engine_handle.await??;
    /// # Ok(())
    /// # }
    /// ```
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
                    activity_executor: Arc::new(WorkerEngineWrapper::new(
                        activity_queue_for_context.clone(),
                    )),
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
                .unwrap_or(5)
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

    /// Spawns a background reaper that moves expired leased items back to the main queue.
    async fn start_reaper_processor(&self) -> tokio::task::JoinHandle<Result<(), WorkerError>> {
        let activity_queue = self.activity_queue.clone();
        let running = self.running.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let interval = Duration::from_secs(self.config.reaper_interval_seconds.unwrap_or(5).max(1));
        let batch_size = self.config.reaper_batch_size.unwrap_or(100);

        tokio::spawn(async move {
            debug!("Starting reaper processor");
            while *running.read().await {
                if *shutdown_rx.borrow() {
                    break;
                }

                if let Err(e) = activity_queue.requeue_expired(batch_size).await {
                    error!(error = %e, "Reaper failed to requeue expired items");
                }

                tokio::select! {
                    _ = tokio::time::sleep(interval) => {},
                    _ = shutdown_rx.changed() => break,
                }
            }
            debug!("Reaper processor stopped");
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
    /// Register an activity handler for a specific activity type.
    ///
    /// This method associates an activity type string with a handler implementation.
    /// Activities of the registered type will be processed by the provided handler.
    ///
    /// # Parameters
    ///
    /// * `activity_type` - String identifier for the activity type
    /// * `activity` - Handler implementation for processing activities of this type
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use runner_q::{WorkerEngine, ActivityHandler, ActivityContext, ActivityHandlerResult};
    /// use async_trait::async_trait;
    /// use serde_json::Value;
    /// use std::sync::Arc;
    ///
    /// // Define a custom activity handler
    /// pub struct EmailHandler;
    ///
    /// #[async_trait]
    /// impl ActivityHandler for EmailHandler {
    ///     async fn handle(&self, payload: Value, _context: ActivityContext) -> ActivityHandlerResult {
    ///         println!("Sending email: {:?}", payload);
    ///         Ok(Some(serde_json::json!({"status": "sent"})))
    ///     }
    ///     
    ///     fn activity_type(&self) -> String {
    ///         "send_email".to_string()
    ///     }
    /// }
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut engine = WorkerEngine::new(redis_pool, config);
    ///
    /// // Register the email handler
    /// engine.register_activity(
    ///     "send_email".to_string(),
    ///     Arc::new(EmailHandler)
    /// );
    ///
    /// // Now activities of type "send_email" will be processed by EmailHandler
    /// # Ok(())
    /// # }
    /// ```
    pub fn register_activity(&mut self, activity_type: String, activity: Arc<dyn ActivityHandler>) {
        self.activity_handlers.insert(activity_type, activity);
    }

    /// Get an activity executor for orchestrating activities from within handlers.
    ///
    /// This method returns an `ActivityExecutor` that can be used by activity handlers
    /// to execute other activities, enabling complex workflows and activity orchestration.
    ///
    /// The returned executor is thread-safe and can be shared across multiple handlers.
    ///
    /// # Returns
    ///
    /// Returns an `Arc<dyn ActivityExecutor>` that can be used to execute activities.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use runner_q::{WorkerEngine, ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityOption, ActivityPriority};
    /// use async_trait::async_trait;
    /// use serde_json::Value;
    /// use std::sync::Arc;
    ///
    /// pub struct OrderProcessingHandler;
    ///
    /// #[async_trait]
    /// impl ActivityHandler for OrderProcessingHandler {
    ///     async fn handle(&self, payload: Value, context: ActivityContext) -> ActivityHandlerResult {
    ///         let order_id = payload["order_id"].as_str().unwrap();
    ///         
    ///         // Execute payment processing activity
    ///         let payment_future = context.activity_executor.execute_activity(
    ///             "process_payment".to_string(),
    ///             serde_json::json!({"order_id": order_id, "amount": payload["amount"]}),
    ///             Some(ActivityOption {
    ///                 priority: Some(ActivityPriority::High),
    ///                 max_retries: 3,
    ///                 timeout_seconds: 300,
    ///                 delay_seconds: None,
    ///             })
    ///         ).await?;
    ///         
    ///         // Execute inventory update activity
    ///         let inventory_future = context.activity_executor.execute_activity(
    ///             "update_inventory".to_string(),
    ///             serde_json::json!({"order_id": order_id, "items": payload["items"]}),
    ///             None // Use default options
    ///         ).await?;
    ///         
    ///         Ok(Some(serde_json::json!({
    ///             "order_id": order_id,
    ///             "status": "processing",
    ///             "sub_activities": ["payment", "inventory"]
    ///         })))
    ///     }
    ///     
    ///     fn activity_type(&self) -> String {
    ///         "process_order".to_string()
    ///     }
    /// }
    /// ```
    pub fn get_activity_executor(&self) -> Arc<dyn ActivityExecutor> {
        Arc::new(WorkerEngineWrapper::new(self.activity_queue.clone()))
    }
}

/// Builder for creating WorkerEngine instances with fluent configuration.
///
/// This builder provides a more ergonomic API for configuring the WorkerEngine
/// with method chaining instead of constructing a WorkerConfig struct directly.
///
/// # Examples
///
/// ```rust,no_run
/// use runner_q::WorkerEngine;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let engine = WorkerEngine::builder()
///     .redis_url("redis://localhost:6379")
///     .queue_name("my_app")
///     .max_workers(8)
///     .schedule_poll_interval(Duration::from_secs(30))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct WorkerEngineBuilder {
    redis_url: Option<String>,
    queue_name: Option<String>,
    max_workers: Option<usize>,
    schedule_poll_interval: Option<Duration>,
    redis_config: Option<RedisConfig>,
    metrics: Option<Arc<dyn MetricsSink>>,
}

impl WorkerEngineBuilder {
    /// Creates a new WorkerEngineBuilder with default values.
    pub fn new() -> Self {
        Self {
            redis_url: None,
            queue_name: None,
            max_workers: None,
            schedule_poll_interval: None,
            redis_config: None,
            metrics: None,
        }
    }

    /// Sets the Redis URL for the worker engine.
    ///
    /// # Parameters
    ///
    /// * `url` - Redis connection URL (e.g., "redis://localhost:6379")
    pub fn redis_url(mut self, url: &str) -> Self {
        self.redis_url = Some(url.to_string());
        self
    }

    /// Sets the queue name for the worker engine.
    ///
    /// # Parameters
    ///
    /// * `name` - Queue name used as Redis key prefix
    pub fn queue_name(mut self, name: &str) -> Self {
        self.queue_name = Some(name.to_string());
        self
    }

    /// Sets the maximum number of concurrent workers.
    ///
    /// # Parameters
    ///
    /// * `max` - Maximum number of concurrent activities
    pub fn max_workers(mut self, max: usize) -> Self {
        self.max_workers = Some(max);
        self
    }

    /// Sets the schedule poll interval for processing scheduled activities.
    ///
    /// # Parameters
    ///
    /// * `interval` - Duration between scheduled activity polls
    pub fn schedule_poll_interval(mut self, interval: Duration) -> Self {
        self.schedule_poll_interval = Some(interval);
        self
    }

    /// Sets the Redis connection pool configuration.
    ///
    /// # Parameters
    ///
    /// * `config` - Redis connection pool configuration
    pub fn redis_config(mut self, config: RedisConfig) -> Self {
        self.redis_config = Some(config);
        self
    }

    /// Sets the metrics sink for monitoring activity processing.
    ///
    /// # Parameters
    ///
    /// * `sink` - Metrics implementation for collecting statistics
    pub fn metrics(mut self, sink: Arc<dyn MetricsSink>) -> Self {
        self.metrics = Some(sink);
        self
    }

    /// Builds the WorkerEngine with the configured settings.
    ///
    /// # Returns
    ///
    /// Returns a `Result<WorkerEngine, WorkerError>` containing the configured engine
    /// or an error if Redis connection fails.
    ///
    /// # Errors
    ///
    /// Returns `WorkerError` if Redis connection cannot be established.
    pub async fn build(self) -> Result<WorkerEngine, WorkerError> {
        let redis_url = self
            .redis_url
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
        let queue_name = self.queue_name.unwrap_or_else(|| "default".to_string());
        let max_concurrent_activities = self.max_workers.unwrap_or(10);
        let schedule_poll_interval_seconds = self.schedule_poll_interval.map_or(5, |d| d.as_secs());

        let config = WorkerConfig {
            queue_name,
            max_concurrent_activities,
            redis_url: redis_url.clone(),
            schedule_poll_interval_seconds: Some(schedule_poll_interval_seconds),
            lease_ms: Some(60_000),
            reaper_interval_seconds: Some(5),
            reaper_batch_size: Some(100),
        };

        let redis_pool = if let Some(redis_config) = self.redis_config {
            create_redis_pool_with_config(&config.redis_url, redis_config).await?
        } else {
            create_redis_pool(&config.redis_url).await?
        };

        let mut worker_engine = WorkerEngine::new(redis_pool, config);

        // Apply custom metrics if provided
        if let Some(metrics) = self.metrics {
            worker_engine.with_metrics(metrics);
        }

        Ok(worker_engine)
    }
}

impl Default for WorkerEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating and executing activities with fluent configuration.
///
/// This builder provides a more ergonomic API for activity execution with method chaining
/// instead of constructing an ActivityOption struct directly.
///
/// # Examples
///
/// ```rust,no_run
/// use runner_q::{WorkerEngine, ActivityPriority};
/// use serde_json::json;
/// use std::time::Duration;
///
/// # async fn example(engine: &WorkerEngine) -> Result<(), Box<dyn std::error::Error>> {
/// let future = engine
///     .activity("send_email")
///     .payload(json!({"to": "user@example.com", "subject": "Hello"}))
///     .priority(ActivityPriority::High)
///     .max_retries(5)
///     .timeout(Duration::from_secs(600))
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ActivityBuilder<'a> {
    engine: &'a WorkerEngineWrapper,
    activity_type: String,
    payload: Option<serde_json::Value>,
    priority: Option<ActivityPriority>,
    max_retries: Option<u32>,
    timeout: Option<Duration>,
    delay: Option<Duration>,
}

impl<'a> ActivityBuilder<'a> {
    /// Creates a new ActivityBuilder for the given engine and activity type.
    pub fn new(engine: &'a WorkerEngineWrapper, activity_type: String) -> Self {
        Self {
            engine,
            activity_type,
            payload: None,
            priority: None,
            max_retries: None,
            timeout: None,
            delay: None,
        }
    }

    /// Sets the payload for the activity.
    ///
    /// # Parameters
    ///
    /// * `payload` - JSON payload containing the activity data
    pub fn payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Sets the priority for the activity.
    ///
    /// # Parameters
    ///
    /// * `priority` - Activity priority level
    pub fn priority(mut self, priority: ActivityPriority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Sets the maximum number of retries for the activity.
    ///
    /// # Parameters
    ///
    /// * `retries` - Maximum number of retry attempts (0 for unlimited)
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Sets the timeout for the activity execution.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Maximum execution time before timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the delay before the activity should be executed.
    ///
    /// # Parameters
    ///
    /// * `delay` - Delay before execution
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Executes the activity with the configured settings.
    ///
    /// # Returns
    ///
    /// Returns a `Result<ActivityFuture, WorkerError>` containing the activity future
    /// or an error if the activity cannot be enqueued.
    ///
    /// # Errors
    ///
    /// Returns `WorkerError` if the activity cannot be enqueued or if there are Redis connection issues.
    pub async fn execute(self) -> Result<ActivityFuture, WorkerError> {
        let payload = self
            .payload
            .ok_or_else(|| WorkerError::QueueError("Activity payload is required".to_string()))?;

        let option = if self.priority.is_some()
            || self.max_retries.is_some()
            || self.timeout.is_some()
            || self.delay.is_some()
        {
            Some(ActivityOption {
                priority: self.priority,
                max_retries: self.max_retries.unwrap_or(3),
                timeout_seconds: self.timeout.map(|d| d.as_secs()).unwrap_or(300),
                delay_seconds: self.delay.map(|d| d.as_secs()),
            })
        } else {
            None
        };

        self.engine
            .execute_activity(self.activity_type, payload, option)
            .await
    }
}

/// Trait for executing activities, enabling activity orchestration.
///
/// This trait allows activity handlers to execute other activities, creating
/// complex workflows and orchestration patterns. It's provided to handlers
/// through the `ActivityContext` to enable nested activity execution.
///
/// # Examples
///
/// ```rust,no_run
/// use runner_q::{ActivityExecutor, ActivityOption, ActivityPriority};
/// use serde_json::json;
/// use std::sync::Arc;
///
/// # async fn example(executor: Arc<dyn ActivityExecutor>) -> Result<(), Box<dyn std::error::Error>> {
/// // Execute a simple activity
/// let future = executor.execute_activity(
///     "send_notification".to_string(),
///     json!({"user_id": 123, "message": "Hello"}),
///     None
/// ).await?;
///
/// // Execute a high-priority activity with custom options
/// let priority_future = executor.execute_activity(
///     "process_payment".to_string(),
///     json!({"amount": 100.0}),
///     Some(ActivityOption {
///         priority: Some(ActivityPriority::High),
///         max_retries: 5,
///         timeout_seconds: 600,
///         delay_seconds: None,
///     })
/// ).await?;
///
/// // Wait for completion
/// if let Ok(Some(result)) = future.get_result().await {
///     println!("Activity completed: {:?}", result);
/// }
/// # Ok(())
/// # }
/// ```
#[async_trait::async_trait]
pub trait ActivityExecutor: Send + Sync {
    /// Creates a fluent activity builder for executing activities with method chaining.
    ///
    /// This provides a more ergonomic API for activity execution with fluent configuration.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use runner_q::{WorkerEngine, ActivityPriority};
    /// use serde_json::json;
    /// use std::time::Duration;
    ///
    /// # async fn example(engine: &WorkerEngine) -> Result<(), Box<dyn std::error::Error>> {
    /// let future = engine
    ///     .activity("send_email")
    ///     .payload(json!({"to": "user@example.com", "subject": "Hello"}))
    ///     .priority(ActivityPriority::High)
    ///     .max_retries(5)
    ///     .timeout(Duration::from_secs(600))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    fn activity(&self, activity_type: &str) -> ActivityBuilder<'_>;
}

/// Wrapper that provides activity execution capabilities to handlers.
///
/// This struct implements `ActivityExecutor` and is used internally to provide
/// activity execution capabilities to activity handlers through the `ActivityContext`.
/// It wraps the underlying activity queue to enable orchestration.
#[derive(Clone)]
pub struct WorkerEngineWrapper {
    activity_queue: Arc<dyn ActivityQueueTrait>,
}

impl WorkerEngineWrapper {
    pub(crate) fn new(activity_queue: Arc<dyn ActivityQueueTrait>) -> Self {
        Self { activity_queue }
    }
    /// Execute an activity and return a future for tracking its completion.
    ///
    /// This method enqueues an activity for processing and returns an `ActivityFuture`
    /// that can be used to wait for the activity's completion and retrieve its result.
    ///
    /// # Parameters
    ///
    /// * `activity_type` - String identifier for the activity type
    /// * `payload` - JSON payload containing the activity data
    /// * `option` - Optional configuration for the activity
    ///
    /// # Returns
    ///
    /// Returns an `ActivityFuture` that can be awaited to get the activity result.
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

#[async_trait::async_trait]
impl ActivityExecutor for WorkerEngineWrapper {
    /// Creates a fluent activity builder for executing activities with method chaining.
    ///
    /// This provides a more ergonomic API for activity execution with fluent configuration.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use runner_q::{WorkerEngine, ActivityPriority};
    /// use serde_json::json;
    /// use std::time::Duration;
    ///
    /// # async fn example(engine: &WorkerEngine) -> Result<(), Box<dyn std::error::Error>> {
    /// let future = engine
    ///     .activity("send_email")
    ///     .payload(json!({"to": "user@example.com", "subject": "Hello"}))
    ///     .priority(ActivityPriority::High)
    ///     .max_retries(5)
    ///     .timeout(Duration::from_secs(600))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    fn activity(&self, activity_type: &str) -> ActivityBuilder<'_> {
        ActivityBuilder::new(self, activity_type.to_string())
    }
}
