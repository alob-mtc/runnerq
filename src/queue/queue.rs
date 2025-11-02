use crate::worker::WorkerError;
use crate::{
    activity::activity::{Activity, ActivityStatus},
    ActivityPriority,
};
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, error, info, warn};

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

    /// Requeue expired processing items back to main queue (reaper)
    async fn requeue_expired(&self, max_to_process: usize) -> Result<u64, WorkerError>;

    /// Extend the lease for an activity in the processing ZSET.
    async fn extend_lease(
        &self,
        activity_id: uuid::Uuid,
        extend_by: std::time::Duration,
    ) -> Result<bool, WorkerError>;

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
    /// Default lease duration in milliseconds used when claiming items
    default_lease_ms: u64,
}

impl ActivityQueue {
    /// Creates a new ActivityQueue backed by the given Redis connection pool and using `queue_name` as the key prefix.
    ///
    /// `queue_name` is used as the prefix for Redis keys (for example: `"<queue_name>:priority_queue"`), so choose a stable, unique name per logical queue.
    ///
    /// # Examples
    ///
    /// ```
    /// let pool: Pool<redis::aio::ConnectionManager> = unimplemented!();
    /// let queue = ActivityQueue::new(pool, "my-activities".to_string());
    /// ```
    pub fn new(
        redis_pool: Pool<RedisConnectionManager>,
        queue_name: String,
        default_lease_ms: u64,
    ) -> Self {
        Self {
            redis_pool,
            queue_name,
            default_lease_ms,
        }
    }

    /// Return the Redis key used for the main priority queue.
    ///
    /// The key is built by appending `":priority_queue"` to the queue's `queue_name`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// // `pool` represents a Redis connection pool available in your context.
    /// let queue = ActivityQueue::new(pool, "my_queue");
    /// let key = queue.get_main_queue_key();
    /// assert_eq!(key, "my_queue:priority_queue");
    /// ```
    fn get_main_queue_key(&self) -> String {
        format!("{}:priority_queue", self.queue_name)
    }

    /// Return the Redis key used for the processing ZSET (holds leases by deadline ms)
    fn get_processing_queue_key(&self) -> String {
        format!("{}:processing", self.queue_name)
    }

    /// Compute a numeric score for the Redis sorted set that encodes priority and FIFO order.
    ///
    /// The returned score is: `priority_weight + (created_at_microseconds % 1_000_000)`.
    /// Priority weights ensure higher-priority activities sort before lower ones when using
    /// ZREVRANGE; the microsecond portion preserves FIFO ordering within the same priority.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Higher priority yields a larger score:
    /// let score_critical = queue.calculate_priority_score(&ActivityPriority::Critical, chrono::Utc::now());
    /// let score_normal = queue.calculate_priority_score(&ActivityPriority::Normal, chrono::Utc::now());
    /// assert!(score_critical > score_normal);
    ///
    /// // Later-created activity within same priority has a larger score (FIFO):
    /// let t1 = chrono::Utc::now();
    /// let t2 = t1 + chrono::Duration::milliseconds(1);
    /// let s1 = queue.calculate_priority_score(&ActivityPriority::Normal, t1);
    /// let s2 = queue.calculate_priority_score(&ActivityPriority::Normal, t2);
    /// assert!(s2 > s1);
    /// ```
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

    /// Create the string stored as a Redis queue entry for an activity.
    ///
    /// The returned value is the activity ID, a colon, then the activity serialized as JSON:
    /// `"<activity_id>:<activity_json>"`. This format is parsed by `parse_queue_entry` when
    /// reading entries from the queue and is used as the member value stored in the priority ZSET.
    ///
    /// # Examples
    ///
    /// ```
    /// // Assuming `queue` implements `create_queue_entry` and `activity` has an `id` field:
    /// let entry = queue.create_queue_entry(&activity);
    /// assert!(entry.starts_with(&activity.id));
    /// ```
    fn create_queue_entry(&self, activity: &Activity) -> String {
        format!(
            "{}:{}",
            activity.id,
            serde_json::to_string(activity).unwrap_or_default()
        )
    }

    /// Parse a queue entry string and deserialize it into an Activity.
    ///
    /// The queue entry must be in the format `<activity_id>:<activity_json>`.
    /// This function extracts the substring after the first colon and attempts
    /// to deserialize it as JSON into an `Activity`.
    ///
    /// Returns a `WorkerError::QueueError` if the entry does not contain a colon
    /// or if the JSON cannot be parsed.
    ///
    /// # Examples
    ///
    /// ```
    /// // Given an entry like "abc123:{\"id\":\"abc123\",\"name\":\"do_work\",\"priority\":\"Normal\"}",
    /// // the JSON portion after the first colon is deserialized into `Activity`.
    /// let entry = r#"abc123:{"id":"abc123","name":"do_work","priority":"Normal"}"#;
    /// let activity_json = &entry[entry.find(':').unwrap() + 1..];
    /// let activity: crate::Activity = serde_json::from_str(activity_json).unwrap();
    /// assert_eq!(activity.id, "abc123");
    /// ```
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

    /// Update the stored status of an activity in Redis.
    ///
    /// Writes the JSON-serialized `status` into the hash key `activity:<activity_id>` under the field `"status"`.
    /// Returns Ok(()) on success or a `WorkerError::QueueError` if a Redis connection cannot be obtained or on serialization/Redis errors.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(queue: &ActivityQueue) -> Result<(), WorkerError> {
    /// let id = uuid::Uuid::new_v4();
    /// queue.update_activity_status(&id, &ActivityStatus::Running).await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Best-effort removal of an activity from processing ZSET (acknowledgement)
    async fn ack_processing(&self, activity_id: &uuid::Uuid) -> Result<(), WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let processing_key = self.get_processing_queue_key();
        let activity_key = format!("activity:{}", activity_id);

        // Try to read exact member stored at claim time
        let member: Option<String> = match conn.hget(&activity_key, "processing_member").await {
            Ok(m) => m,
            Err(e) => {
                error!(activity_id = %activity_id, error = %e, "ack_processing: HGET processing_member failed");
                None
            }
        };

        if let Some(m) = member {
            match conn.zrem::<_, _, i32>(&processing_key, &m).await {
                Ok(removed) => {
                    if removed == 0 {
                        warn!(activity_id = %activity_id, "ack_processing: member not found in processing ZSET during ZREM");
                    }
                }
                Err(e) => {
                    error!(activity_id = %activity_id, error = %e, "ack_processing: ZREM processing member failed");
                }
            }

            if let Err(e) = conn
                .hdel::<_, _, ()>(&activity_key, ("processing_member", "lease_deadline_ms"))
                .await
            {
                error!(activity_id = %activity_id, error = %e, "ack_processing: HDEL of processing fields failed");
            }
            return Ok(());
        }

        // No fallback: avoid unbounded scans that can OOM. Missing processing_member indicates
        // state drift; rely on the reaper to clean up.
        error!(
            activity_id = %activity_id,
            "ack_processing: missing processing_member; skipping unbounded scan and leaving cleanup to reaper"
        );

        Ok(())
    }
}

#[async_trait]
impl ActivityQueueTrait for ActivityQueue {
    /// Enqueue an activity into the Redis-backed priority queue.
    ///
    /// Adds the given `activity` to the main ZSET using a score computed from the activity's
    /// priority and creation timestamp (ensuring priority ordering with FIFO within the same
    /// priority). Also stores activity metadata in a Redis hash (including status, retry count,
    /// priority, and computed score) and sets a 24-hour TTL on that metadata.
    ///
    /// Returns `Ok(())` on success or a `WorkerError::QueueError` (or other mapped error) on failure.
    ///
    /// # Examples
    ///
    /// ```
    /// # use futures::executor::block_on;
    /// # // Setup omitted: create `queue: ActivityQueue` and an `activity: Activity`.
    /// # block_on(async {
    /// // enqueue an activity asynchronously
    /// queue.enqueue(activity).await.unwrap();
    /// # });
    /// ```
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

        // Set TTL for activity metadata (7 days)
        let days = 7 * 24 * 60 * 60;
        let _: () = conn.expire(&activity_key, days).await?;

        info!(
            activity_id = %activity.id,
            activity_type = ?activity.activity_type,
            priority = ?activity.priority,
            score = %score,
            "Activity enqueued with optimized priority queue"
        );
        Ok(())
    }

    /// Dequeues the highest-priority ready activity, waiting up to `timeout`.
    ///
    /// Attempts to remove the top-scoring entry from the queue and, on success,
    /// marks the activity's status as `Running` in Redis before returning it.
    /// If no activity becomes available within `timeout`, returns `Ok(None)`.
    ///
    /// Notes:
    /// - The method performs a non-blocking polling loop (with exponential backoff)
    ///   against the Redis sorted set and uses an atomic remove attempt to claim an
    ///   entry. On success the activity is parsed and its status persisted as
    ///   `Running`.
    /// - Side effects: updates activity metadata in Redis (status -> `Running`)
    ///   and removes the dequeued entry from the main queue.
    ///
    /// # Parameters
    /// - `timeout`: total duration to wait for an activity before returning `None`.
    ///
    /// # Returns
    /// `Ok(Some(Activity))` when an activity was claimed and updated to `Running`,
    /// `Ok(None)` if the timeout elapsed without any available activity, or
    /// `Err(WorkerError)` for Redis/parse errors.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(q: &ActivityQueue) -> Result<(), WorkerError> {
    /// let timeout = std::time::Duration::from_secs(5);
    /// if let Some(activity) = q.dequeue(timeout).await? {
    ///     // process `activity`
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn dequeue(&self, timeout: Duration) -> Result<Option<Activity>, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let queue_key = self.get_main_queue_key();
        let processing_key = self.get_processing_queue_key();

        // Poll with exponential backoff until timeout, performing atomic claim via Lua
        let start_time = std::time::Instant::now();
        let mut sleep_duration = Duration::from_millis(10);

        while start_time.elapsed() < timeout {
            // Atomic claim script: ZPOPMAX main -> ZADD processing with deadline
            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
            let lease_ms = self.default_lease_ms;
            let lua = r#"
            local item = redis.call('ZPOPMAX', KEYS[1])
            if (item == nil) or (#item == 0) then return nil end
            local member = item[1]
            local deadline = tonumber(ARGV[1]) + tonumber(ARGV[2])
            redis.call('ZADD', KEYS[2], deadline, member)
            return member
            "#;

            let claimed: Option<String> = redis::cmd("EVAL")
                .arg(lua)
                .arg(2)
                .arg(&queue_key)
                .arg(&processing_key)
                .arg(now_ms)
                .arg(lease_ms)
                .query_async(&mut *conn)
                .await?;

            if let Some(queue_entry) = claimed {
                // Successfully claimed; parse and mark running
                let mut activity = self.parse_queue_entry(&queue_entry)?;
                activity.status = ActivityStatus::Running;

                // Update activity status and remember processing member for ack
                self.update_activity_status(&activity.id, &activity.status)
                    .await?;
                let activity_key = format!("activity:{}", activity.id);
                let _: () = conn
                    .hset_multiple(
                        &activity_key,
                        &[
                            ("processing_member", queue_entry.as_str()),
                            (
                                "lease_deadline_ms",
                                (now_ms + lease_ms).to_string().as_str(),
                            ),
                        ],
                    )
                    .await?;

                // Extend lease if it's less than the timeout
                if lease_ms / 1000 < activity.timeout_seconds {
                    let delta = activity.timeout_seconds - (lease_ms / 1000);
                    if delta > 0 {
                        self.extend_lease(activity.id, Duration::from_secs(delta))
                            .await?;
                    }
                }

                debug!(
                    activity_id = %activity.id,
                    activity_type = ?activity.activity_type,
                    priority = ?activity.priority,
                    "Activity claimed with lease and moved to processing"
                );
                return Ok(Some(activity));
            }

            // No activities available, wait with exponential backoff
            tokio::time::sleep(sleep_duration).await;
            sleep_duration = std::cmp::min(sleep_duration * 2, Duration::from_millis(1000));
        }

        Ok(None) // Timeout
    }

    /// Mark an activity as completed by updating its stored status.
    ///
    /// Updates the activity's status to `Completed` (persisted in Redis) and returns when the update succeeds.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(queue: &ActivityQueue) -> Result<(), WorkerError> {
    /// let activity_id = uuid::Uuid::new_v4();
    /// queue.mark_completed(activity_id).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn mark_completed(&self, activity_id: uuid::Uuid) -> Result<(), WorkerError> {
        // Ack from processing first to avoid leaks
        self.ack_processing(&activity_id).await?;
        self.update_activity_status(&activity_id, &ActivityStatus::Completed)
            .await?;
        info!(activity_id = %activity_id, "Activity marked as completed");
        Ok(())
    }

    /// Handle a failed activity by either scheduling a retry with exponential backoff or moving it to the dead-letter queue.
    ///
    /// If `retryable` is false the activity status is set to `Failed`. If `retryable` is true and the activity has
    /// remaining retries (either `max_retries == 0` meaning unlimited or `retry_count < max_retries`), the function
    /// increments `retry_count`, sets the status to `Retrying`, computes an exponential backoff delay as
    /// `retry_delay_seconds * 2.pow(retry_count)`, sets `scheduled_at` to now + delay, and schedules the activity.
    /// When retries are exhausted the status is set to `DeadLetter` and the activity is pushed to the dead-letter queue.
    ///
    /// Returns `Ok(())` on success or a `WorkerError::QueueError` if underlying Redis operations fail.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chrono::Utc;
    /// # use tokio_test::block_on;
    /// # async fn doc_example(queue: &crate::queue::ActivityQueue) {
    /// let activity = crate::Activity {
    ///     id: "a1".to_string(),
    ///     retry_count: 0,
    ///     max_retries: 3,
    ///     retry_delay_seconds: 5,
    ///     status: crate::ActivityStatus::Pending,
    ///     created_at: Utc::now(),
    ///     scheduled_at: None,
    ///     priority: crate::Priority::Normal,
    ///     payload: serde_json::json!({}),
    /// };
    /// let _ = queue.mark_failed(activity, "transient error".to_string(), true).await;
    /// # }
    /// ```
    async fn mark_failed(
        &self,
        activity: Activity,
        error_message: String,
        retryable: bool,
    ) -> Result<(), WorkerError> {
        let activity_id = activity.id;

        if !retryable {
            // Ack and mark failed
            self.ack_processing(&activity_id).await?;
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
            // Ack current processing before re-scheduling
            self.ack_processing(&activity_id).await?;
            self.schedule_activity(retry_activity).await?;
            info!(activity_id = %activity_id, retry_count = retry_count, "Activity scheduled for retry");
        } else {
            // Move to dead letter queue
            self.ack_processing(&activity_id).await?;
            self.update_activity_status(&activity_id, &ActivityStatus::DeadLetter)
                .await?;
            self.add_to_dead_letter_queue(activity, error_message)
                .await?;
            error!(activity_id = %activity_id, "Activity moved to dead letter queue");
        }

        Ok(())
    }

    /// Schedule an activity for future execution by adding it to the Redis `scheduled_activities` ZSET.
    ///
    /// The activity is serialized to JSON and inserted into the `scheduled_activities` sorted set with
    /// the Unix timestamp (seconds) from `activity.scheduled_at` as the score. If `scheduled_at` is
    /// `None`, the current UTC time is used instead.
    ///
    /// # Errors
    ///
    /// Returns `WorkerError::QueueError` when acquiring a Redis connection or performing Redis
    /// operations fails, and when activity serialization fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chrono::Utc;
    /// # use uuid::Uuid;
    /// # async fn example(queue: &impl crate::queue::ActivityQueueTrait) -> Result<(), crate::errors::WorkerError> {
    /// let activity = crate::queue::Activity {
    ///     id: Uuid::new_v4(),
    ///     created_at: Utc::now(),
    ///     scheduled_at: Some(Utc::now()),
    ///     // ... other fields ...
    /// };
    /// queue.schedule_activity(activity).await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Process scheduled activities whose scheduled time has arrived.
    ///
    /// This fetches up to 100 entries from the `scheduled_activities` sorted set with scores up to the current
    /// Unix timestamp, removes each found entry from the scheduled set, deserializes it into an `Activity`,
    /// sets its status to `Pending`, re-enqueues it on the main priority queue, and returns the list of
    /// activities that were successfully processed.
    ///
    /// Parsing errors for individual scheduled entries are logged and skipped; Redis and enqueue failures
    /// propagate as `WorkerError::QueueError`.
    ///
    /// # Returns
    ///
    /// A `Vec<Activity>` containing the activities that were processed and re-enqueued.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use your_crate::{ActivityQueue, WorkerError};
    /// # async fn example(queue: ActivityQueue) -> Result<(), WorkerError> {
    /// let ready = queue.process_scheduled_activities().await?;
    /// // ready now holds activities whose scheduled time has arrived and have been re-enqueued
    /// # Ok(())
    /// # }
    /// ```
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

    /// Requeue expired items from processing back to main priority queue.
    ///
    /// This function requeues expired items from the processing ZSET back to the main priority queue.
    ///
    /// # Parameters
    /// - `max_to_process`: The maximum number of items to requeue.
    ///
    /// # Returns
    /// - `Ok(u64)` the number of items requeued.
    /// - `WorkerError::QueueError` if the Redis connection fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(queue: &ActivityQueue) -> Result<(), WorkerError> {
    /// let max_to_process = 100;
    /// let requeued = queue.requeue_expired(max_to_process).await?;
    /// assert!(requeued > 0);
    /// # Ok(())
    /// # }
    /// ```
    async fn requeue_expired(&self, max_to_process: usize) -> Result<u64, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let processing_key = self.get_processing_queue_key();
        let main_key = self.get_main_queue_key();
        let now_ms = chrono::Utc::now().timestamp_millis();

        let lua = r#"
        local now = tonumber(ARGV[1])
        local limit = tonumber(ARGV[2])
        local moved = 0
        local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)

        for _, m in ipairs(members) do
        if redis.call('ZREM', KEYS[1], m) == 1 then
            local colon = string.find(m, ':', 1, true)
            if colon then
            local id = string.sub(m, 1, colon - 1)
            local activity_key = 'activity:' .. id
            local score = redis.call('HGET', activity_key, 'score')
            if not score then score = now end
            redis.call('ZADD', KEYS[2], score, m)
            redis.call('HDEL', activity_key, 'processing_member', 'lease_deadline_ms')
            redis.call('HSET', activity_key, 'status', '\"Pending\"')
            else
            -- Fallback if format unexpected
            redis.call('ZADD', KEYS[2], now, m)
            end
            moved = moved + 1
        end
        end
        return moved
        "#;

        let moved: i64 = redis::cmd("EVAL")
            .arg(lua)
            .arg(2)
            .arg(&processing_key)
            .arg(&main_key)
            .arg(now_ms)
            .arg(max_to_process as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e| WorkerError::QueueError(format!("requeue_expired EVAL failed: {}", e)))?;

        Ok(moved as u64)
    }

    /// Extend the lease for an activity in the processing ZSET.
    ///
    /// This function extends the lease for an activity in the processing ZSET by the specified duration.
    ///
    /// # Parameters
    /// - `activity_id`: The ID of the activity to extend the lease for.
    /// - `extend_by`: The duration to extend the lease by.
    ///
    /// # Returns
    /// - `Ok(true)` if the lease was extended successfully.
    /// - `Ok(false)` if the activity was not found in the processing ZSET.
    /// - `WorkerError::QueueError` if the Redis connection fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn example(queue: &ActivityQueue) -> Result<(), WorkerError> {
    /// let activity_id = uuid::Uuid::new_v4();
    /// let extend_by = std::time::Duration::from_secs(60);
    /// let extended = queue.extend_lease(activity_id, extend_by).await?;
    /// assert!(extended);
    /// # Ok(())
    /// # }
    /// ```
    async fn extend_lease(
        &self,
        activity_id: uuid::Uuid,
        extend_by: Duration,
    ) -> Result<bool, WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let processing_key = self.get_processing_queue_key();
        let activity_key = format!("activity:{}", activity_id);

        // We stored this when claiming
        let member: Option<String> = conn.hget(&activity_key, "processing_member").await?;
        if member.is_none() {
            return Ok(false);
        }
        let member = member.unwrap();

        let extend_ms = extend_by.as_millis() as i64;

        // Atomically extend from current deadline
        let lua = r#"
        local member = ARGV[1]
        local extend_ms = tonumber(ARGV[2])

        local current_deadline = redis.call('ZSCORE', KEYS[1], member)
        if current_deadline == false then
          return 0
        end

        local new_deadline = tonumber(current_deadline) + extend_ms
        redis.call('ZADD', KEYS[1], 'XX', new_deadline, member)
        redis.call('HSET', KEYS[2], 'lease_deadline_ms', tostring(new_deadline))
        return new_deadline
        "#;

        let new_deadline: i64 = redis::cmd("EVAL")
            .arg(lua)
            .arg(2)
            .arg(&processing_key)
            .arg(&activity_key)
            .arg(&member)
            .arg(extend_ms)
            .query_async(&mut *conn)
            .await
            .map_err(|e| WorkerError::QueueError(format!("extend_lease EVAL failed: {}", e)))?;

        Ok(new_deadline > 0)
    }

    /// Returns aggregated queue statistics (counts) for the Redis-backed activity queue.
    ///
    /// This queries Redis for:
    /// - total pending activities in the main priority ZSET,
    /// - per-priority counts computed by ZSET score ranges (Critical/High/Normal/Low),
    /// - number of scheduled activities in the `scheduled_activities` ZSET,
    /// - number of entries in the `dead_letter_queue` list.
    ///
    /// The priority counts rely on the queue's score layout:
    /// Critical: 4_000_000–4_999_999, High: 3_000_000–3_999_999, Normal: 2_000_000–2_999_999, Low: 1_000_000–1_999_999.
    ///
    /// Returns a `QueueStats` on success, or a `WorkerError::QueueError` if obtaining a Redis connection or executing Redis commands fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn run_example(queue: &crate::queue::ActivityQueue) {
    /// let stats = queue.get_stats().await.unwrap();
    /// println!("pending: {}", stats.pending_activities);
    /// # }
    /// ```
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

    /// Store an activity's result in Redis under the key `result:<activity_id>` with a 24-hour TTL.
    ///
    /// The `result` is serialized to JSON and written to Redis. On success returns `Ok(())`.
    /// On failure, returns `Err(WorkerError::QueueError)` for Redis/connection issues or
    /// serialization errors.
    ///
    /// # Parameters
    /// - `activity_id`: UUID used to construct the Redis key `result:<activity_id>`.
    /// - `result`: The ActivityResult to serialize and persist.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uuid::Uuid;
    /// # async fn doc_example(queue: &crate::queue::ActivityQueue) -> Result<(), crate::WorkerError> {
    /// let id = Uuid::new_v4();
    /// let result = crate::queue::ActivityResult {
    ///     data: None,
    ///     state: crate::queue::ResultState::Ok,
    /// };
    /// queue.store_result(id, result).await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Retrieves a previously stored activity result by activity ID.
    ///
    /// Returns `Ok(Some(ActivityResult))` if a serialized result exists for `activity_id`,
    /// `Ok(None)` if no result is stored, or an error if Redis access or JSON deserialization fails.
    /// Errors are returned as `WorkerError::QueueError`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use uuid::Uuid;
    /// # use my_crate::queue::ActivityQueue;
    /// # #[tokio::test]
    /// # async fn example_get_result() {
    /// let queue = /* ActivityQueue::new(...) */ unimplemented!();
    /// let id = Uuid::new_v4();
    /// let res = queue.get_result(id).await;
    /// // `res` will be `Ok(None)` if no result was stored for `id`.
    /// # }
    /// ```
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
            60_000,
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
