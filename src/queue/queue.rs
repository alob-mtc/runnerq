use crate::worker::WorkerError;
use crate::{
    activity::activity::{Activity, ActivityStatus},
    ActivityPriority,
};
use async_trait::async_trait;
use bb8_redis::bb8::PooledConnection;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

/// Trait defining the interface for activity queue operations
#[async_trait]
pub(crate) trait ActivityQueueTrait: Send + Sync {
    /// Enqueue an activity for processing
    async fn enqueue(&self, activity: Activity) -> Result<(), WorkerError>;

    /// Dequeue the next available activity with timeout
    async fn dequeue(&self, timeout: Duration) -> Result<Option<Activity>, WorkerError>;

    /// Assign a worker to an activity that has been dequeued.
    async fn assign_worker(
        &self,
        activity_id: uuid::Uuid,
        worker_id: &str,
    ) -> Result<(), WorkerError>;

    /// Mark a activity as completed
    async fn mark_completed(
        &self,
        activity: &Activity,
        worker_id: Option<&str>,
    ) -> Result<(), WorkerError>;

    /// Mark a activity as failed and optionally requeue for retry
    async fn mark_failed(
        &self,
        activity: Activity,
        error_message: String,
        retryable: bool,
        worker_id: Option<&str>,
    ) -> Result<(), WorkerError>;

    /// Schedule an activity for future execution
    async fn schedule_activity(&self, activity: Activity) -> Result<(), WorkerError>;

    /// Process scheduled activities that are ready to run
    async fn process_scheduled_activities(&self) -> Result<Vec<Activity>, WorkerError>;

    /// Requeue expired processing items back to main queue (reaper)
    async fn requeue_expired(&self, max_to_process: usize) -> Result<u64, WorkerError>;

    #[allow(dead_code)]
    /// Extend the lease for an activity in the processing ZSET.
    async fn extend_lease(
        &self,
        activity_id: uuid::Uuid,
        extend_by: std::time::Duration,
    ) -> Result<bool, WorkerError>;

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
    event_tx: Arc<RwLock<Option<broadcast::Sender<ActivityEvent>>>>,
}

impl ActivityQueue {
    const SNAPSHOT_TTL_SECONDS: i64 = 7 * 24 * 60 * 60;
    const EVENT_TTL_SECONDS: i64 = Self::SNAPSHOT_TTL_SECONDS;
    const EVENT_MAX_LEN: isize = 200;

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
            event_tx: Arc::new(RwLock::new(None)),
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

    pub fn redis_pool(&self) -> Pool<RedisConnectionManager> {
        self.redis_pool.clone()
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn set_event_channel(&self, sender: broadcast::Sender<ActivityEvent>) {
        if let Ok(mut guard) = self.event_tx.write() {
            *guard = Some(sender);
        }
    }

    pub fn event_channel(&self) -> Option<broadcast::Sender<ActivityEvent>> {
        self.event_tx
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
    }

    fn get_activity_key(&self, activity_id: &uuid::Uuid) -> String {
        format!("activity:{}", activity_id)
    }

    fn get_activity_events_key(&self, activity_id: &uuid::Uuid) -> String {
        format!("activity:{}:events", activity_id)
    }

    fn now() -> DateTime<Utc> {
        Utc::now()
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

    async fn write_snapshot(
        &self,
        conn: &mut PooledConnection<'_, RedisConnectionManager>,
        snapshot: &ActivitySnapshot,
    ) -> Result<(), WorkerError> {
        let activity_key = self.get_activity_key(&snapshot.id);
        let mut fields: Vec<(String, String)> = Vec::new();

        fields.push(("snapshot".to_string(), serde_json::to_string(snapshot)?));
        fields.push((
            "status".to_string(),
            serde_json::to_string(&snapshot.status)?,
        ));
        fields.push((
            "priority".to_string(),
            serde_json::to_string(&snapshot.priority)?,
        ));
        fields.push(("activity_type".to_string(), snapshot.activity_type.clone()));
        fields.push((
            "payload".to_string(),
            serde_json::to_string(&snapshot.payload)?,
        ));
        fields.push(("created_at".to_string(), snapshot.created_at.to_rfc3339()));
        fields.push((
            "scheduled_at".to_string(),
            snapshot
                .scheduled_at
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default(),
        ));
        fields.push((
            "started_at".to_string(),
            snapshot
                .started_at
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default(),
        ));
        fields.push((
            "completed_at".to_string(),
            snapshot
                .completed_at
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default(),
        ));
        fields.push((
            "current_worker_id".to_string(),
            snapshot.current_worker_id.clone().unwrap_or_default(),
        ));
        fields.push((
            "last_worker_id".to_string(),
            snapshot.last_worker_id.clone().unwrap_or_default(),
        ));
        fields.push(("retry_count".to_string(), snapshot.retry_count.to_string()));
        fields.push(("max_retries".to_string(), snapshot.max_retries.to_string()));
        fields.push((
            "timeout_seconds".to_string(),
            snapshot.timeout_seconds.to_string(),
        ));
        fields.push((
            "retry_delay_seconds".to_string(),
            snapshot.retry_delay_seconds.to_string(),
        ));
        fields.push((
            "metadata".to_string(),
            serde_json::to_string(&snapshot.metadata)?,
        ));
        fields.push((
            "last_error".to_string(),
            snapshot.last_error.clone().unwrap_or_default(),
        ));
        fields.push((
            "last_error_at".to_string(),
            snapshot
                .last_error_at
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default(),
        ));
        fields.push((
            "status_updated_at".to_string(),
            snapshot.status_updated_at.to_rfc3339(),
        ));
        fields.push((
            "score".to_string(),
            snapshot.score.map(|s| s.to_string()).unwrap_or_default(),
        ));
        fields.push((
            "lease_deadline_ms".to_string(),
            snapshot
                .lease_deadline_ms
                .map(|s| s.to_string())
                .unwrap_or_default(),
        ));
        fields.push((
            "processing_member".to_string(),
            snapshot.processing_member.clone().unwrap_or_default(),
        ));

        let _: () = conn.hset_multiple(&activity_key, &fields).await?;
        let _: () = conn
            .expire(&activity_key, Self::SNAPSHOT_TTL_SECONDS)
            .await?;
        Ok(())
    }

    async fn load_snapshot(
        &self,
        conn: &mut PooledConnection<'_, RedisConnectionManager>,
        activity_id: &uuid::Uuid,
    ) -> Result<ActivitySnapshot, WorkerError> {
        let activity_key = self.get_activity_key(activity_id);
        let snapshot_json: Option<String> = conn.hget(&activity_key, "snapshot").await?;
        if let Some(json) = snapshot_json {
            serde_json::from_str(&json).map_err(|e| {
                WorkerError::QueueError(format!("Failed to deserialize activity snapshot: {}", e))
            })
        } else {
            Err(WorkerError::QueueError(format!(
                "No snapshot found for activity {}",
                activity_id
            )))
        }
    }

    async fn record_event(
        &self,
        conn: &mut PooledConnection<'_, RedisConnectionManager>,
        activity_id: &uuid::Uuid,
        event_type: ActivityEventType,
        worker_id: Option<&str>,
        detail: Option<Value>,
    ) -> Result<(), WorkerError> {
        let activity_event = ActivityEvent {
            activity_id: *activity_id,
            timestamp: Self::now(),
            event_type,
            worker_id: worker_id.map(|s| s.to_string()),
            detail,
        };

        let events_key = self.get_activity_events_key(activity_id);
        let payload = serde_json::to_string(&activity_event)?;
        let _: () = conn.rpush(&events_key, payload).await?;
        let _: () = conn.ltrim(&events_key, -Self::EVENT_MAX_LEN, -1).await?;
        let _: () = conn.expire(&events_key, Self::EVENT_TTL_SECONDS).await?;

        if let Some(tx) = self
            .event_tx
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
        {
            let _ = tx.send(activity_event.clone());
        }

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

        // Persist snapshot with expanded metadata for observability
        let mut snapshot = ActivitySnapshot::from_activity(&activity);
        snapshot.score = Some(score);
        snapshot.status_updated_at = Self::now();
        self.write_snapshot(&mut conn, &snapshot).await?;

        // Record lifecycle event
        self.record_event(
            &mut conn,
            &activity.id,
            ActivityEventType::Enqueued,
            None,
            Some(json!({
                "priority": snapshot.priority,
                "scheduled_at": snapshot.scheduled_at,
            })),
        )
        .await?;

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

                let activity_key = self.get_activity_key(&activity.id);
                let lease_deadline_ms = (now_ms + lease_ms) as i64;

                let meta_fields = vec![
                    ("processing_member".to_string(), queue_entry.clone()),
                    (
                        "lease_deadline_ms".to_string(),
                        lease_deadline_ms.to_string(),
                    ),
                ];
                let _: () = conn.hset_multiple(&activity_key, &meta_fields).await?;

                let mut snapshot = match self.load_snapshot(&mut conn, &activity.id).await {
                    Ok(existing) => existing,
                    Err(_) => ActivitySnapshot::from_activity(&activity),
                };

                let started_at = Self::now();
                snapshot.update_status(ActivityStatus::Running, started_at);
                snapshot.mark_started(started_at);
                snapshot.lease_deadline_ms = Some(lease_deadline_ms);
                snapshot.processing_member = Some(queue_entry.clone());
                snapshot.score =
                    Some(self.calculate_priority_score(&activity.priority, activity.created_at));

                self.write_snapshot(&mut conn, &snapshot).await?;

                self.record_event(
                    &mut conn,
                    &activity.id,
                    ActivityEventType::Dequeued,
                    None,
                    Some(json!({
                        "lease_deadline_ms": lease_deadline_ms,
                    })),
                )
                .await?;

                // Extend lease if it's less than the timeout
                if lease_ms / 1000 < activity.timeout_seconds {
                    // let delta = activity.timeout_seconds - (lease_ms / 1000);
                    // if delta > 0 {
                    //     self.extend_lease(activity.id, Duration::from_secs(delta))
                    //         .await?;
                    // }
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

    async fn assign_worker(
        &self,
        activity_id: uuid::Uuid,
        worker_id: &str,
    ) -> Result<(), WorkerError> {
        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let mut snapshot = self.load_snapshot(&mut conn, &activity_id).await?;
        let assigned_at = Self::now();
        if snapshot.started_at.is_none() {
            snapshot.mark_started(assigned_at);
        }
        snapshot.set_current_worker_id(Some(worker_id.to_string()));
        snapshot.set_last_worker_id(Some(worker_id.to_string()));
        snapshot.update_status(ActivityStatus::Running, assigned_at);

        self.write_snapshot(&mut conn, &snapshot).await?;
        self.record_event(
            &mut conn,
            &activity_id,
            ActivityEventType::Started,
            Some(worker_id),
            None,
        )
        .await?;

        Ok(())
    }

    /// Mark an activity as completed by updating its stored status.
    ///
    /// Updates the activity's status to `Completed` (persisted in Redis) and returns when the update succeeds.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(queue: &ActivityQueue, activity: &crate::Activity) -> Result<(), WorkerError> {
    /// queue.mark_completed(activity, Some("worker-1")).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn mark_completed(
        &self,
        activity: &Activity,
        worker_id: Option<&str>,
    ) -> Result<(), WorkerError> {
        // Ack from processing first to avoid leaks
        self.ack_processing(&activity.id).await?;

        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let mut snapshot = match self.load_snapshot(&mut conn, &activity.id).await {
            Ok(s) => s,
            Err(_) => ActivitySnapshot::from_activity(activity),
        };
        let completed_at = Self::now();
        snapshot.update_status(ActivityStatus::Completed, completed_at);
        snapshot.mark_completed(completed_at);
        if let Some(worker) = worker_id {
            snapshot.set_last_worker_id(Some(worker.to_string()));
        }
        snapshot.set_current_worker_id(None);
        snapshot.processing_member = None;
        snapshot.lease_deadline_ms = None;
        snapshot.set_last_error(None, None);

        self.write_snapshot(&mut conn, &snapshot).await?;
        self.record_event(
            &mut conn,
            &activity.id,
            ActivityEventType::Completed,
            worker_id,
            Some(json!({
                "activity_type": activity.activity_type,
            })),
        )
        .await?;

        info!(activity_id = %activity.id, "Activity marked as completed");
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
    /// let _ = queue
    ///     .mark_failed(activity, "transient error".to_string(), true, None)
    ///     .await;
    /// # }
    /// ```
    async fn mark_failed(
        &self,
        activity: Activity,
        error_message: String,
        retryable: bool,
        worker_id: Option<&str>,
    ) -> Result<(), WorkerError> {
        let activity_id = activity.id;

        let mut conn = self.redis_pool.get().await.map_err(|e| {
            WorkerError::QueueError(format!("Failed to get Redis connection: {}", e))
        })?;

        let mut snapshot = match self.load_snapshot(&mut conn, &activity_id).await {
            Ok(s) => s,
            Err(_) => ActivitySnapshot::from_activity(&activity),
        };
        let now = Self::now();
        if let Some(worker) = worker_id {
            snapshot.set_last_worker_id(Some(worker.to_string()));
        }
        snapshot.set_current_worker_id(None);
        snapshot.processing_member = None;
        snapshot.lease_deadline_ms = None;
        snapshot.set_last_error(Some(error_message.clone()), Some(now));

        if !retryable {
            // Ack and mark failed
            self.ack_processing(&activity_id).await?;
            snapshot.update_status(ActivityStatus::Failed, now);
            snapshot.mark_completed(now);
            self.write_snapshot(&mut conn, &snapshot).await?;
            self.record_event(
                &mut conn,
                &activity_id,
                ActivityEventType::Failed,
                worker_id,
                Some(json!({
                    "retryable": false,
                    "error": error_message,
                })),
            )
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
            snapshot.update_status(ActivityStatus::Retrying, now);
            snapshot.retry_count = retry_count;
            snapshot.scheduled_at = Some(scheduled_at);
            self.write_snapshot(&mut conn, &snapshot).await?;
            self.record_event(
                &mut conn,
                &activity_id,
                ActivityEventType::Retrying,
                worker_id,
                Some(json!({
                    "retry_count": retry_count,
                    "scheduled_at": scheduled_at,
                    "error": error_message,
                })),
            )
            .await?;
            self.schedule_activity(retry_activity).await?;
            info!(activity_id = %activity_id, retry_count = retry_count, "Activity scheduled for retry");
        } else {
            // Move to dead letter queue
            self.ack_processing(&activity_id).await?;
            snapshot.update_status(ActivityStatus::DeadLetter, now);
            snapshot.mark_completed(now);
            self.write_snapshot(&mut conn, &snapshot).await?;
            let dead_letter_error = error_message.clone();
            self.record_event(
                &mut conn,
                &activity_id,
                ActivityEventType::DeadLetter,
                worker_id,
                Some(json!({
                    "error": error_message,
                })),
            )
            .await?;
            self.add_to_dead_letter_queue(activity, dead_letter_error)
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

        let mut snapshot = match self.load_snapshot(&mut conn, &activity.id).await {
            Ok(s) => s,
            Err(_) => ActivitySnapshot::from_activity(&activity),
        };
        let scheduled_dt = activity.scheduled_at.unwrap_or_else(chrono::Utc::now);
        snapshot.scheduled_at = Some(scheduled_dt);
        snapshot.retry_count = activity.retry_count;
        snapshot.update_status(activity.status.clone(), Self::now());
        snapshot.set_current_worker_id(None);
        snapshot.processing_member = None;
        snapshot.lease_deadline_ms = None;
        self.write_snapshot(&mut conn, &snapshot).await?;
        self.record_event(
            &mut conn,
            &activity.id,
            ActivityEventType::Scheduled,
            None,
            Some(json!({
                "scheduled_at": scheduled_dt,
                "status": activity.status,
            })),
        )
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
        local moved_ids = {}
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
            table.insert(moved_ids, id)
            else
            redis.call('ZADD', KEYS[2], now, m)
            end
        end
        end
        return moved_ids
        "#;

        let moved_ids: Vec<String> = redis::cmd("EVAL")
            .arg(lua)
            .arg(2)
            .arg(&processing_key)
            .arg(&main_key)
            .arg(now_ms)
            .arg(max_to_process as i64)
            .query_async(&mut *conn)
            .await
            .map_err(|e| WorkerError::QueueError(format!("requeue_expired EVAL failed: {}", e)))?;

        for id_str in &moved_ids {
            if let Ok(activity_id) = uuid::Uuid::parse_str(id_str) {
                if let Ok(mut snapshot) = self.load_snapshot(&mut conn, &activity_id).await {
                    let transition_at = Self::now();
                    snapshot.update_status(ActivityStatus::Pending, transition_at);
                    snapshot.set_current_worker_id(None);
                    snapshot.processing_member = None;
                    snapshot.lease_deadline_ms = None;
                    self.write_snapshot(&mut conn, &snapshot).await.ok();
                    self.record_event(
                        &mut conn,
                        &activity_id,
                        ActivityEventType::Requeued,
                        None,
                        Some(json!({ "reason": "lease_expired" })),
                    )
                    .await
                    .ok();
                }
            }
        }

        Ok(moved_ids.len() as u64)
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

        let mut snapshot = self.load_snapshot(&mut conn, &activity_id).await?;
        snapshot.lease_deadline_ms = Some(new_deadline);
        self.write_snapshot(&mut conn, &snapshot).await?;
        self.record_event(
            &mut conn,
            &activity_id,
            ActivityEventType::LeaseExtended,
            None,
            Some(json!({
                "new_deadline_ms": new_deadline,
                "extend_by_ms": extend_ms,
            })),
        )
        .await?;

        Ok(new_deadline > 0)
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
        let result_value = serde_json::to_value(&result)?;
        let result_json = result_value.to_string();

        // Store result with TTL (24 hours)
        let _: () = conn.set_ex(&result_key, result_json, 86400).await?;

        self.record_event(
            &mut conn,
            &activity_id,
            ActivityEventType::ResultStored,
            None,
            Some(result_value),
        )
        .await?;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivitySnapshot {
    pub id: uuid::Uuid,
    pub activity_type: String,
    pub payload: serde_json::Value,
    pub priority: ActivityPriority,
    pub status: ActivityStatus,
    pub created_at: DateTime<Utc>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub current_worker_id: Option<String>,
    pub last_worker_id: Option<String>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub timeout_seconds: u64,
    pub retry_delay_seconds: u64,
    pub metadata: HashMap<String, String>,
    pub last_error: Option<String>,
    pub last_error_at: Option<DateTime<Utc>>,
    pub status_updated_at: DateTime<Utc>,
    pub score: Option<f64>,
    pub lease_deadline_ms: Option<i64>,
    pub processing_member: Option<String>,
}

impl ActivitySnapshot {
    pub(crate) fn from_activity(activity: &Activity) -> Self {
        Self {
            id: activity.id,
            activity_type: activity.activity_type.clone(),
            payload: activity.payload.clone(),
            priority: activity.priority.clone(),
            status: activity.status.clone(),
            created_at: activity.created_at,
            scheduled_at: activity.scheduled_at,
            started_at: None,
            completed_at: None,
            current_worker_id: None,
            last_worker_id: None,
            retry_count: activity.retry_count,
            max_retries: activity.max_retries,
            timeout_seconds: activity.timeout_seconds,
            retry_delay_seconds: activity.retry_delay_seconds,
            metadata: activity.metadata.clone(),
            last_error: None,
            last_error_at: None,
            status_updated_at: Utc::now(),
            score: None,
            lease_deadline_ms: None,
            processing_member: None,
        }
    }

    pub fn update_status(&mut self, status: ActivityStatus, timestamp: DateTime<Utc>) {
        self.status = status;
        self.status_updated_at = timestamp;
    }

    pub fn mark_started(&mut self, started_at: DateTime<Utc>) {
        self.started_at = Some(started_at);
    }

    pub fn mark_completed(&mut self, completed_at: DateTime<Utc>) {
        self.completed_at = Some(completed_at);
    }

    pub fn set_current_worker_id(&mut self, worker_id: Option<String>) {
        self.current_worker_id = worker_id;
    }

    pub fn set_last_worker_id(&mut self, worker_id: Option<String>) {
        self.last_worker_id = worker_id;
    }

    pub fn set_last_error(&mut self, error: Option<String>, at: Option<DateTime<Utc>>) {
        self.last_error = error;
        self.last_error_at = at;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActivityEventType {
    Enqueued,
    Scheduled,
    Dequeued,
    Started,
    Completed,
    Failed,
    Retrying,
    DeadLetter,
    Requeued,
    LeaseExtended,
    ResultStored,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEvent {
    pub activity_id: uuid::Uuid,
    pub timestamp: DateTime<Utc>,
    pub event_type: ActivityEventType,
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub pending_activities: u64,
    pub processing_activities: u64,
    pub critical_priority: u64,
    pub high_priority: u64,
    pub normal_priority: u64,
    pub low_priority: u64,
    pub scheduled_activities: u64,
    pub dead_letter_activities: u64,
    pub max_workers: Option<usize>,
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
