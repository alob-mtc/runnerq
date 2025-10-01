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
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

pub struct WorkerEngine {
    activity_queue: Arc<dyn ActivityQueueTrait>,
    activity_handlers: ActivityHandlerRegistry,
    config: WorkerConfig,
    running: Arc<RwLock<bool>>,
}

impl WorkerEngine {
    pub fn new(redis_pool: Pool<RedisConnectionManager>, config: WorkerConfig) -> Self {
        Self {
            activity_queue: Arc::new(ActivityQueue::new(redis_pool, config.queue_name.clone())),
            activity_handlers: ActivityHandlerRegistry::new(),
            config,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Starts the worker engine, spawning the scheduled-activities processor and a set of worker loops.
    ///
    /// This method:
    /// - Fails with `WorkerError::AlreadyRunning` if the engine is already running.
    /// - Marks the engine as running and creates a semaphore limiting concurrent activity execution to
    ///   `config.max_concurrent_activities`.
    /// - Spawns the scheduled activities processor and `max_concurrent_activities` worker loops.
    /// - Waits for either a shutdown signal (Ctrl+C / SIGTERM) or for the worker tasks to complete.
    /// - Calls `stop()` to clear the running flag and perform shutdown teardown before returning.
    ///
    /// # Returns
    ///
    /// `Ok(())` on graceful shutdown; `Err(WorkerError::AlreadyRunning)` if the engine was already running;
    /// other `WorkerError` variants may be returned for underlying failures.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run_example() -> Result<(), WorkerError> {
    /// // Prepare a WorkerEngine (pseudo-code; replace with real init)
    /// let engine = WorkerEngine::new(redis_pool, config);
    /// // Start the engine (this call awaits until shutdown)
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

        // Create a semaphore to limit concurrent activity
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.config.max_concurrent_activities,
        ));
        let mut join_handles = Vec::new();

        // Start scheduled activities processor
        let scheduled_processor_handle = self.start_scheduled_activities_processor().await;
        join_handles.push(scheduled_processor_handle);

        // Start worker loops
        for worker_id in 0..self.config.max_concurrent_activities {
            let handle = self.start_worker_loop(worker_id, semaphore.clone()).await;
            join_handles.push(handle);
        }

        // Wait for shutdown signal or worker completion
        tokio::select! {
            _ = self.wait_for_shutdown() => {
                info!("Shutdown signal received, stopping worker engine");
            }
            result = futures::future::try_join_all(join_handles) => {
                match result {
                    Ok(_) => info!("All worker loops completed"),
                    Err(e) => error!(error = %e, "Worker loop failed"),
                }
            }
        }

        self.stop().await;

        info!("Worker engine stopped");
        Ok(())
    }

    /// Stop the worker engine gracefully
    pub async fn stop(&self) {
        info!("Stopping worker engine");
        let mut running = self.running.write().await;
        *running = false;
    }

    /// Spawns a background worker task that continuously dequeues and executes activities.
    ///
    /// The spawned task runs until the engine's `running` flag is cleared. Each iteration
    /// acquires a permit from the provided semaphore (to limit concurrency), attempts to
    /// dequeue an activity, locates a registered handler for the activity type, and executes
    /// the handler with a per-activity timeout. Results are recorded via the activity queue:
    /// - Successful results are marked completed and stored as `ResultState::Ok`.
    /// - Handlers returning `ActivityError::Retry` cause the activity to be requeued/marked for retry.
    /// - Handlers returning `ActivityError::NonRetry` mark the activity as failed and store a structured
    ///   error result (`{"error": <reason>, "type": "non_retryable", "failed_at": <RFC3339>}`) with state `Err`.
    /// - Executions that exceed the activity timeout are treated as failures and marked for retry.
    ///
    /// The function returns a `JoinHandle` for the spawned task so callers can await or detach it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use tokio::sync::Semaphore;
    /// # // `engine` is an instance of `WorkerEngine` already configured and with handlers registered.
    /// # async fn usage_example(engine: &crate::runner::WorkerEngine) {
    /// let sem = Arc::new(Semaphore::new(4));
    /// let handle = engine.start_worker_loop(0, sem.clone()).await;
    /// // `handle` is a JoinHandle for the worker task; it will run until `engine.stop()` is called.
    /// let _ = handle; // keep or await as needed
    /// # }
    /// ```
    async fn start_worker_loop(
        &self,
        worker_id: usize,
        semaphore: Arc<tokio::sync::Semaphore>,
    ) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let activity_queue = self.activity_queue.clone();
        let activity_handlers = self.activity_handlers.clone();
        let activity_queue_for_context = self.activity_queue.clone();

        tokio::spawn(async move {
            debug!(%worker_id, "Starting worker loop");

            while *running.read().await {
                // Acquire semaphore permit
                let permit = match semaphore.try_acquire() {
                    Ok(permit) => permit,
                    Err(_) => {
                        // No permits available, wait a bit
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

                // Try to get an activity from the queue
                let activity = match activity_queue.dequeue(Duration::from_secs(1)).await {
                    Ok(Some(activity)) => activity,
                    Ok(None) => {
                        // No activity available, continue
                        drop(permit);
                        continue;
                    }
                    Err(e) => {
                        error!(%worker_id, error = %e, "Failed to dequeue activity");
                        drop(permit);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                // Process the activity
                let activity_id = activity.id;
                let activity_type = activity.activity_type.clone();

                debug!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, "Worker processing activity");

                // Find handler for activity type
                let handler = match activity_handlers.get(&activity.activity_type) {
                    Some(handler) => handler.clone(),
                    None => {
                        error!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, "No handler found for activity type");
                        drop(permit);
                        continue;
                    }
                };

                // Create activity context
                let context = ActivityContext {
                    activity_id,
                    activity_type: activity_type.clone(),
                    retry_count: activity.retry_count,
                    metadata: activity.metadata.clone(),
                    worker_engine: Arc::new(WorkerEngineWrapper {
                        activity_queue: activity_queue_for_context.clone(),
                    }),
                };

                // Execute activity with timeout
                let activity_timeout = Duration::from_secs(activity.timeout_seconds);

                let result = tokio::time::timeout(
                    activity_timeout,
                    handler.handle(activity.payload.clone(), context),
                )
                .await;

                match result {
                    Ok(activity_result) => match activity_result {
                        Ok(value) => {
                            if let Err(e) = activity_queue.mark_completed(activity.id).await {
                                error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to mark activity as completed");
                            }
                            info!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, "Activity completed successfully");
                            // Store the result in the result queue
                            let activity_result = ActivityResult {
                                data: value,
                                state: ResultState::Ok,
                            };
                            if let Err(e) = activity_queue
                                .store_result(activity.id, activity_result)
                                .await
                            {
                                error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to store activity result");
                            }
                        }
                        Err(e) => match e {
                            ActivityError::Retry(reason) => {
                                warn!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, reason = %reason, "Activity requesting retry");
                                if let Err(e) =
                                    activity_queue.mark_failed(activity, reason, true).await
                                {
                                    error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to mark activity for retry");
                                }
                            }
                            ActivityError::NonRetry(reason) => {
                                error!(%worker_id, activity_id = %activity_id, activity_type = ?activity_type, reason = %reason, "Activity failed");
                                if let Err(e) = activity_queue
                                    .mark_failed(activity, reason.clone(), false)
                                    .await
                                {
                                    error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to mark activity as failed");
                                }
                                // Store the error result in the result queue
                                let activity_result = ActivityResult {
                                    data: Some(json!({
                                        "error": reason,
                                        "type": "non_retryable",
                                        "failed_at": chrono::Utc::now().to_rfc3339()
                                    })),
                                    state: ResultState::Err,
                                };
                                if let Err(e) = activity_queue
                                    .store_result(activity_id, activity_result)
                                    .await
                                {
                                    error!(%worker_id, activity_id = %activity_id, error = %e, "Failed to store activity result");
                                }
                            }
                        },
                    },
                    Err(_) => {
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
        })
    }

    /// Spawns a background task that repeatedly processes scheduled activities.
    ///
    /// The spawned task loops while the engine's `running` flag is true, calling
    /// `process_scheduled_activities()` on the engine's activity queue and sleeping
    /// for 30 seconds between iterations. Errors returned from the queue are
    /// logged but do not stop the loop. The function returns the Tokio
    /// `JoinHandle` for the spawned background task so the caller can await or
    /// abort it if needed.
    ///
    /// # Examples
    ///
    /// ```
    /// // Assuming `engine` is an initialized `WorkerEngine`:
    /// let handle = engine.start_scheduled_activities_processor().await;
    /// // The background processor is now running; later you can abort or await:
    /// handle.abort(); // or handle.await.ok();
    /// ```
    async fn start_scheduled_activities_processor(&self) -> tokio::task::JoinHandle<()> {
        let activity_queue = self.activity_queue.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            debug!("Starting scheduled activities processor");

            while *running.read().await {
                if let Err(e) = activity_queue.process_scheduled_activities().await {
                    error!(error = %e, "Failed to process scheduled activities");
                }

                // Check for scheduled activities every 30 seconds
                tokio::time::sleep(Duration::from_secs(30)).await;
            }

            debug!("Scheduled activities processor stopped");
        })
    }

    async fn wait_for_shutdown(&self) {
        // Wait for graceful shutdown signals
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
            _ = ctrl_c => {
                info!("Received Ctrl+C signal");
            },
            _ = terminate => {
                info!("Received SIGTERM signal");
            },
        }

        // Signal shutdown
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
        self.activity_queue.enqueue(activity).await?;
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
        self.activity_queue.enqueue(activity).await?;
        Ok(ActivityFuture::new(
            self.activity_queue.clone(),
            activity_id,
        ))
    }
}
