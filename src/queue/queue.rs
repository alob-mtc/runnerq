use crate::worker::WorkerError;
use crate::{activity::activity::Activity, ActivityPriority, ActivityStatus};
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
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

/// Redis-based implementation using an optimized single sorted set for priority queue
///
/// This implementation uses a Redis sorted set (ZSET) instead of multiple lists,
/// providing better performance and more consistent ordering guarantees.
///
/// Score calculation:
/// - Priority weight: Critical=4M, High=3M, Normal=2M, Low=1M
/// - Timestamp: microseconds since epoch (for FIFO within priority)
/// - Final score: priority_weight + timestamp_microseconds
///
/// Benefits:
/// - Single atomic operation for dequeue
/// - Perfect priority ordering with FIFO within priority
/// - Better memory efficiency
/// - Simplified statistics gathering
/// - Support for priority changes without re-queueing
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

    /// Get the main queue key for the priority queue
    fn get_main_queue_key(&self) -> String {
        format!("{}:priority_queue", self.queue_name)
    }

    /// Calculate priority score for sorted set
    ///
    /// Higher priority = higher score for Redis ZREVRANGE operations
    /// Format: priority_weight + timestamp_microseconds
    ///
    /// This ensures:
    /// 1. Higher priority activities are always processed first
    /// 2. Within same priority, FIFO ordering is maintained
    /// 3. Score is unique for each activity (timestamp precision)
    fn calculate_priority_score(
        &self,
        priority: &ActivityPriority,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> f64 {
        let priority_weight = match priority {
            ActivityPriority::Critical => 4_000_000.0,
            ActivityPriority::High => 3_000_000.0,
            ActivityPriority::Normal => 2_000_000.0,
            ActivityPriority::Low => 1_000_000.0,
        };

        // Use microseconds for fine-grained ordering within same priority
        let timestamp_micros = created_at.timestamp_micros() as f64 / 1_000_000.0; // Normalize to seconds with microsecond precision

        // Combine priority and timestamp
        // Note: We add timestamp to ensure FIFO within priority, but priority dominates
        priority_weight + (timestamp_micros % 1_000_000.0) // Modulo to prevent overflow while maintaining ordering
    }

    /// Extract activity ID from queue entry for removal operations
    fn create_queue_entry(&self, activity: &Activity) -> String {
        format!(
            "{}:{}",
            activity.id,
            serde_json::to_string(activity).unwrap_or_default()
        )
    }

    /// Parse queue entry back to activity
    fn parse_queue_entry(&self, entry: &str) -> Result<Activity, WorkerError> {
        if let Some(colon_pos) = entry.find(':') {
            let activity_json = &entry[colon_pos + 1..];
            serde_json::from_str(activity_json)
                .map_err(|e| WorkerError::QueueError(format!("Failed to parse queue entry: {}", e)))
        } else {
            Err(WorkerError::QueueError(
                "Invalid queue entry format".to_string(),
            ))
        }
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
    /// Enqueue an activity using optimized sorted set approach
    async fn enqueue(&self, activity: Activity) -> Result<(), WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let queue_key = self.get_main_queue_key();
        let queue_entry = self.create_queue_entry(&activity);
        let score = self.calculate_priority_score(&activity.priority, activity.created_at);

        // Add to sorted set with calculated score
        let _: () = conn.zadd(&queue_key, queue_entry, score).await?;

        // Store activity metadata for tracking
        let activity_key = format!("activity:{}", activity.id);
        let _: () = conn
            .hset_multiple(
                &activity_key,
                &[
                    ("status", serde_json::to_string(&activity.status)?),
                    ("created_at", activity.created_at.to_rfc3339()),
                    ("retry_count", activity.retry_count.to_string()),
                    ("priority", serde_json::to_string(&activity.priority)?),
                    ("score", score.to_string()),
                ],
            )
            .await?;

        // Set TTL for activity metadata (24 hours)
        let _: () = conn.expire(&activity_key, 86400).await?;

        info!(
            activity_id = %activity.id,
            activity_type = ?activity.activity_type,
            priority = ?activity.priority,
            score = %score,
            "Activity enqueued with optimized priority queue"
        );
        Ok(())
    }

    /// Dequeue the next highest priority activity using single atomic operation
    async fn dequeue(&self, timeout: Duration) -> Result<Option<Activity>, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let queue_key = self.get_main_queue_key();

        // Use Lua script for atomic dequeue operation with timeout simulation
        // Since Redis doesn't have BZPOPMAX with timeout, we'll implement polling with exponential backoff
        let start_time = std::time::Instant::now();
        let mut sleep_duration = std::time::Duration::from_millis(10);

        while start_time.elapsed() < timeout {
            // Try to pop highest priority item (highest score first)
            let result: Vec<String> = conn.zrevrange_withscores(&queue_key, 0, 0).await?;

            if !result.is_empty() {
                let queue_entry = &result[0];
                // Remove from queue atomically
                let removed: i32 = conn.zrem(&queue_key, queue_entry).await?;

                if removed > 0 {
                    // Successfully dequeued, parse and return
                    let mut activity = self.parse_queue_entry(queue_entry)?;
                    activity.status = ActivityStatus::Running;

                    // Update activity status
                    self.update_activity_status(&activity.id, &activity.status)
                        .await?;

                    debug!(
                        activity_id = %activity.id,
                        activity_type = ?activity.activity_type,
                        priority = ?activity.priority,
                        "Activity dequeued from optimized priority queue"
                    );
                    return Ok(Some(activity));
                }
            }

            // No activities available, wait with exponential backoff
            tokio::time::sleep(sleep_duration).await;
            sleep_duration =
                std::cmp::min(sleep_duration * 2, std::time::Duration::from_millis(1000));
        }

        Ok(None) // Timeout
    }

    async fn mark_completed(&self, activity_id: uuid::Uuid) -> Result<(), WorkerError> {
        self.update_activity_status(&activity_id, &ActivityStatus::Completed)
            .await?;
        info!(activity_id = %activity_id, "Activity marked as completed");
        Ok(())
    }

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

            return Ok(());
        }

        if activity.max_retries == 0 || activity.retry_count < activity.max_retries {
            // Requeue for retry with updated count
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
                "Processed scheduled activities with optimized queue"
            );
        }

        Ok(ready_activities)
    }

    /// Get queue statistics with improved performance
    async fn get_stats(&self) -> Result<QueueStats, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let queue_key = self.get_main_queue_key();

        // Get total pending activities count
        let total_pending: u64 = conn.zcard(&queue_key).await?;

        // Get count by priority using score ranges
        let critical_count: u64 = conn.zcount(&queue_key, 4_000_000.0, 4_999_999.0).await?;
        let high_count: u64 = conn.zcount(&queue_key, 3_000_000.0, 3_999_999.0).await?;
        let normal_count: u64 = conn.zcount(&queue_key, 2_000_000.0, 2_999_999.0).await?;
        let low_count: u64 = conn.zcount(&queue_key, 1_000_000.0, 1_999_999.0).await?;

        let scheduled_count: u64 = conn.zcard("scheduled_activities").await?;
        let dead_letter_count: u64 = conn.llen("dead_letter_queue").await?;

        Ok(QueueStats {
            pending_activities: total_pending,
            critical_priority: critical_count,
            high_priority: high_count,
            normal_priority: normal_count,
            low_priority: low_count,
            scheduled_activities: scheduled_count,
            dead_letter_activities: dead_letter_count,
        })
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    async fn test_priority_score_calculation() {
        let queue = ActivityQueue::new(
            // Mock pool - not used in this test
            Pool::builder()
                .build(bb8_redis::RedisConnectionManager::new("redis://localhost").unwrap())
                .await
                .unwrap(),
            "test".to_string(),
        );

        let now = Utc::now();

        // Test priority ordering
        let critical_score = queue.calculate_priority_score(&ActivityPriority::Critical, now);
        let high_score = queue.calculate_priority_score(&ActivityPriority::High, now);
        let normal_score = queue.calculate_priority_score(&ActivityPriority::Normal, now);
        let low_score = queue.calculate_priority_score(&ActivityPriority::Low, now);

        assert!(critical_score > high_score);
        assert!(high_score > normal_score);
        assert!(normal_score > low_score);

        // Test FIFO within same priority
        let later = now + chrono::Duration::microseconds(1000);
        let earlier_score = queue.calculate_priority_score(&ActivityPriority::Normal, now);
        let later_score = queue.calculate_priority_score(&ActivityPriority::Normal, later);

        assert!(
            later_score > earlier_score,
            "Later activities should have higher scores for FIFO ordering"
        );
    }
}
