use crate::worker::WorkerError;
use crate::{activity::activity::Activity, ActivityPriority, ActivityStatus};
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json;
use std::time::Duration;
use tracing::{debug, error, info};

/// Trait defining the interface for activity queue operations
#[async_trait]
pub(crate) trait ActivityQueueTrait: Send + Sync {
    /// Enqueue an activity for processing
    async fn enqueue(&self, activity: Activity) -> Result<(), WorkerError>;

    /// Dequeue the next available activity with timeout
    async fn dequeue(&self, timeout: Duration) -> Result<Option<Activity>, WorkerError>;

    /// Mark a activity as completed
    async fn mark_completed(&self, activity_id: uuid::Uuid) -> Result<(), WorkerError>;

    /// Mark a activity as failed and optionally requeue for retry
    async fn mark_failed(
        &self,
        activity: Activity,
        error_message: String,
        retryable: bool,
    ) -> Result<(), WorkerError>;

    /// Schedule an activity for future execution
    async fn schedule_activity(&self, activity: Activity) -> Result<(), WorkerError>;

    /// Process scheduled activities that are ready to run
    async fn process_scheduled_activities(&self) -> Result<Vec<Activity>, WorkerError>;

    #[allow(dead_code)]
    /// Get queue statistics
    async fn get_stats(&self) -> Result<QueueStats, WorkerError>;

    /// Store activity result
    async fn store_result(
        &self,
        activity_id: uuid::Uuid,
        result: ActivityResult,
    ) -> Result<(), WorkerError>;

    /// Retrieve activity result
    async fn get_result(
        &self,
        activity_id: uuid::Uuid,
    ) -> Result<Option<ActivityResult>, WorkerError>;
}

/// Redis-based implementation of ActivityQueueTrait
#[derive(Clone)]
pub struct ActivityQueue {
    redis_pool: Pool<RedisConnectionManager>,
    queue_name: String,
}

impl ActivityQueue {
    pub fn new(redis_pool: Pool<RedisConnectionManager>, queue_name: String) -> Self {
        Self {
            redis_pool,
            queue_name,
        }
    }

    fn get_queue_key(&self, priority: &ActivityPriority) -> String {
        format!("{}:{:?}", self.queue_name, priority).to_lowercase()
    }

    async fn update_activity_status(
        &self,
        activity_id: &uuid::Uuid,
        status: &ActivityStatus,
    ) -> Result<(), WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let activity_key = format!("activity:{}", activity_id);
        let _: () = conn
            .hset(&activity_key, "status", serde_json::to_string(status)?)
            .await?;

        Ok(())
    }

    async fn add_to_dead_letter_queue(
        &self,
        activity: Activity,
        error_message: String,
    ) -> Result<(), WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let dead_letter_entry = serde_json::json!({
            "activity": activity,
            "error": error_message,
            "failed_at": chrono::Utc::now()
        });

        let _: () = conn
            .rpush(
                "dead_letter_queue",
                serde_json::to_string(&dead_letter_entry)?,
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl ActivityQueueTrait for ActivityQueue {
    /// Enqueue a activity for processing
    async fn enqueue(&self, activity: Activity) -> Result<(), WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let activity_json = serde_json::to_string(&activity)?;
        let queue_key = self.get_queue_key(&activity.priority);

        // Add activity to priority queue
        let _: () = conn.rpush(&queue_key, &activity_json).await?;

        // Store activity metadata for tracking
        let activity_key = format!("activity:{}", activity.id);
        let _: () = conn
            .hset_multiple(
                &activity_key,
                &[
                    ("status", serde_json::to_string(&activity.status)?),
                    ("created_at", activity.created_at.to_rfc3339()),
                    ("retry_count", activity.retry_count.to_string()),
                ],
            )
            .await?;

        // Set TTL for activity metadata (24 hours)
        let _: () = conn.expire(&activity_key, 86400).await?;

        info!(activity_id = %activity.id, activity_type = ?activity.activity_type, "Activity enqueued");
        Ok(())
    }

    /// Dequeue the next available activity
    async fn dequeue(&self, timeout: Duration) -> Result<Option<Activity>, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        // Check queues by priority (highest first)
        let queue_keys = vec![
            self.get_queue_key(&ActivityPriority::Critical),
            self.get_queue_key(&ActivityPriority::High),
            self.get_queue_key(&ActivityPriority::Normal),
            self.get_queue_key(&ActivityPriority::Low),
        ];

        // Use BLPOP to wait for activity across all priority queues
        let result: Option<(String, String)> =
            conn.blpop(&queue_keys, timeout.as_secs_f64()).await?;

        match result {
            Some((_queue, activity_json)) => {
                let mut activity: Activity = serde_json::from_str(&activity_json)?;
                activity.status = ActivityStatus::Running;

                // Update activity status
                self.update_activity_status(&activity.id, &activity.status)
                    .await?;

                debug!(activity_id = %activity.id, activity_type = ?activity.activity_type, "Activity dequeued");
                Ok(Some(activity))
            }
            None => Ok(None), // Timeout
        }
    }

    /// Mark a activity as completed
    async fn mark_completed(&self, activity_id: uuid::Uuid) -> Result<(), WorkerError> {
        self.update_activity_status(&activity_id, &ActivityStatus::Completed)
            .await?;
        info!(activity_id = %activity_id, "Activity marked as completed");
        Ok(())
    }

    /// Mark a activity as failed and optionally requeue for retry
    async fn mark_failed(
        &self,
        activity: Activity,
        error_message: String,
        retryable: bool,
    ) -> Result<(), WorkerError> {
        let activity_id = activity.id;
        if !retryable {
            self.update_activity_status(&activity_id, &ActivityStatus::Failed)
                .await?;
        }

        if activity.max_retries == 0 || activity.retry_count < activity.max_retries {
            // Requeue for retry
            let mut retry_activity = activity;
            retry_activity.retry_count += 1;
            retry_activity.status = ActivityStatus::Retrying;

            // Add exponential backoff delay
            let delay_seconds =
                retry_activity.retry_delay_seconds * 2_u64.pow(retry_activity.retry_count);
            let scheduled_at = chrono::Utc::now() + chrono::Duration::seconds(delay_seconds as i64);
            retry_activity.scheduled_at = Some(scheduled_at);

            let retry_count = retry_activity.retry_count;
            self.schedule_activity(retry_activity).await?;
            info!(activity_id = %activity_id, retry_count = retry_count, "Activity scheduled for retry");
        } else {
            // Move to dead letter queue
            self.update_activity_status(&activity_id, &ActivityStatus::DeadLetter)
                .await?;
            self.add_to_dead_letter_queue(activity, error_message)
                .await?;
            error!(activity_id = %activity_id, "Activity moved to dead letter queue");
        }

        Ok(())
    }

    /// Schedule a activity for future execution
    async fn schedule_activity(&self, activity: Activity) -> Result<(), WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let activity_json = serde_json::to_string(&activity)?;
        let scheduled_key = "scheduled_activities";

        let scheduled_at = activity
            .scheduled_at
            .unwrap_or_else(chrono::Utc::now)
            .timestamp();

        // Add to sorted set with timestamp as score
        let _: () = conn
            .zadd(scheduled_key, activity_json, scheduled_at)
            .await?;

        debug!(activity_id = %activity.id, scheduled_at = %scheduled_at, "Activity scheduled");
        Ok(())
    }

    /// Process scheduled activities that are ready to run
    async fn process_scheduled_activities(&self) -> Result<Vec<Activity>, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let now = chrono::Utc::now().timestamp();
        let scheduled_key = "scheduled_activities";

        // Get activities that are ready to run
        let activity_jsons: Vec<String> = conn
            .zrangebyscore_limit(scheduled_key, 0, now, 0, 100)
            .await?;

        let mut ready_activities = Vec::new();

        for activity_json in activity_jsons {
            // Remove from scheduled set
            let _: () = conn.zrem(scheduled_key, &activity_json).await?;

            // Parse and enqueue activity
            match serde_json::from_str::<Activity>(&activity_json) {
                Ok(mut activity) => {
                    activity.status = ActivityStatus::Pending;
                    self.enqueue(activity.clone()).await?;
                    ready_activities.push(activity);
                }
                Err(e) => {
                    error!(error = %e, "Failed to parse scheduled activity");
                }
            }
        }

        if !ready_activities.is_empty() {
            info!(
                count = ready_activities.len(),
                "Processed scheduled activities"
            );
        }

        Ok(ready_activities)
    }

    /// Get queue statistics
    async fn get_stats(&self) -> Result<QueueStats, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let critical_count: u64 = conn
            .llen(self.get_queue_key(&ActivityPriority::Critical))
            .await?;
        let high_count: u64 = conn
            .llen(self.get_queue_key(&ActivityPriority::High))
            .await?;
        let normal_count: u64 = conn
            .llen(self.get_queue_key(&ActivityPriority::Normal))
            .await?;
        let low_count: u64 = conn
            .llen(self.get_queue_key(&ActivityPriority::Low))
            .await?;
        let scheduled_count: u64 = conn.zcard("scheduled_activities").await?;
        let dead_letter_count: u64 = conn.llen("dead_letter_queue").await?;

        Ok(QueueStats {
            pending_activities: critical_count + high_count + normal_count + low_count,
            critical_priority: critical_count,
            high_priority: high_count,
            normal_priority: normal_count,
            low_priority: low_count,
            scheduled_activities: scheduled_count,
            dead_letter_activities: dead_letter_count,
        })
    }

    /// Store activity result
    async fn store_result(
        &self,
        activity_id: uuid::Uuid,
        result: ActivityResult,
    ) -> Result<(), WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let result_key = format!("result:{}", activity_id);
        let result_json = serde_json::to_value(result)?.to_string();

        // Store result with TTL (24 hours)
        let _: () = conn.set_ex(&result_key, result_json, 86400).await?;

        info!(activity_id = %activity_id, "Activity result stored");
        Ok(())
    }

    /// Retrieve activity result
    async fn get_result(
        &self,
        activity_id: uuid::Uuid,
    ) -> Result<Option<ActivityResult>, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let result_key = format!("result:{}", activity_id);
        let result_json: Option<String> = conn.get(&result_key).await?;

        match result_json {
            Some(json) => {
                let result: ActivityResult = serde_json::from_str(&json)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

#[derive(Debug)]
pub struct QueueStats {
    pub pending_activities: u64,
    pub critical_priority: u64,
    pub high_priority: u64,
    pub normal_priority: u64,
    pub low_priority: u64,
    pub scheduled_activities: u64,
    pub dead_letter_activities: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum ResultState {
    Ok,
    Err,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct ActivityResult {
    pub data: Option<serde_json::Value>,
    pub state: ResultState,
}
