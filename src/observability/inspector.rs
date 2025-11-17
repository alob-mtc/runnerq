use std::ops::RangeInclusive;
use std::time::Duration;

use bb8_redis::{
    bb8::{Pool, PooledConnection},
    RedisConnectionManager,
};
use chrono::{DateTime, Utc};
use futures::stream::{Stream, StreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};
use uuid::Uuid;

use crate::activity::activity::{Activity, ActivityStatus};
use crate::queue::queue::{ActivityEvent, ActivitySnapshot, QueueStats};
use crate::runner::error::WorkerError;

#[derive(Clone)]
pub struct QueueInspector {
    redis_pool: Pool<RedisConnectionManager>,
    queue_name: String,
    max_workers: Option<usize>,
}

impl QueueInspector {
    pub fn new(redis_pool: Pool<RedisConnectionManager>, queue_name: impl Into<String>) -> Self {
        Self {
            redis_pool,
            queue_name: queue_name.into(),
            max_workers: None,
        }
    }

    pub fn with_max_workers(mut self, max_workers: usize) -> Self {
        self.max_workers = Some(max_workers);
        self
    }

    /// Create a stream that reads events directly from Redis Streams.
    ///
    /// This stream reads from the global events stream and yields ActivityEvent items.
    /// It's designed to be used directly with SSE endpoints, bypassing in-memory channels.
    /// This enables cross-process event streaming.
    pub fn event_stream(&self) -> impl Stream<Item = Result<ActivityEvent, WorkerError>> {
        let redis_pool = self.redis_pool.clone();
        let stream_key = format!("{}:events_stream", self.queue_name);
        let queue_name = self.queue_name.clone();

        futures::stream::unfold(
            (redis_pool, stream_key, queue_name, "$".to_string()),
            move |state| async move {
                let (pool, key, qn, mut last_id) = state;

                // Get Redis connection
                let mut conn = match pool.get().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(
                            queue_name = %qn,
                            error = %e,
                            "Failed to get Redis connection for event stream"
                        );
                        // Wait a bit before retrying
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        return Some((
                            Err(WorkerError::QueueError(format!(
                                "Redis connection error: {}",
                                e
                            ))),
                            (pool.clone(), key.clone(), qn.clone(), last_id),
                        ));
                    }
                };

                // Read from stream using XREAD
                let result: Result<redis::streams::StreamReadReply, redis::RedisError> = conn
                    .xread_options(
                        &[key.as_str()],
                        &[last_id.as_str()],
                        &redis::streams::StreamReadOptions::default()
                            .count(50) // Read up to 50 events at a time
                            .block(1000), // Block for 1 second if no events
                    )
                    .await;

                match result {
                    Ok(reply) => {
                        // Process each stream in the reply
                        for stream_key_reply in reply.keys {
                            for stream_id in stream_key_reply.ids {
                                last_id = stream_id.id.clone();

                                // Extract the event data
                                for (field, value) in &stream_id.map {
                                    if field == "event" {
                                        if let redis::Value::Data(data) = value {
                                            match String::from_utf8(data.clone()) {
                                                Ok(json_str) => {
                                                    match serde_json::from_str::<ActivityEvent>(
                                                        &json_str,
                                                    ) {
                                                        Ok(event) => {
                                                            return Some((
                                                                Ok(event),
                                                                (
                                                                    pool.clone(),
                                                                    key.clone(),
                                                                    qn.clone(),
                                                                    last_id,
                                                                ),
                                                            ));
                                                        }
                                                        Err(e) => {
                                                            warn!(
                                                                queue_name = %qn,
                                                                stream_id = %stream_id.id,
                                                                error = %e,
                                                                "Failed to parse event from stream"
                                                            );
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!(
                                                        queue_name = %qn,
                                                        stream_id = %stream_id.id,
                                                        error = %e,
                                                        "Failed to decode event data from stream"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        // No events in this batch, continue
                        Some((
                            Err(WorkerError::QueueError("No events".to_string())),
                            (pool.clone(), key.clone(), qn.clone(), last_id),
                        ))
                    }
                    Err(e) if e.kind() == redis::ErrorKind::IoError => {
                        // Connection error - wait and retry
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        Some((
                            Err(WorkerError::QueueError("Redis IO error".to_string())),
                            (pool.clone(), key.clone(), qn.clone(), last_id),
                        ))
                    }
                    Err(e) => {
                        warn!(
                            queue_name = %qn,
                            error = %e,
                            "Error reading from Redis stream"
                        );
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        Some((
                            Err(WorkerError::QueueError(format!(
                                "Redis stream error: {}",
                                e
                            ))),
                            (pool.clone(), key.clone(), qn.clone(), last_id),
                        ))
                    }
                }
            },
        )
        .filter_map(|result| async move {
            match result {
                Ok(event) => Some(Ok(event)),
                Err(_) => {
                    // Filter out errors - continue streaming on errors
                    None
                }
            }
        })
    }

    pub async fn list_pending(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let mut conn = self.connection().await?;
        let range = self.slice_to_range(offset, limit);
        let members: Vec<String> = conn
            .zrevrange(self.main_queue_key(), *range.start(), *range.end())
            .await
            .map_err(Self::map_redis_error)?;
        self.collect_snapshots(&mut conn, &members).await
    }

    pub async fn list_processing(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let mut conn = self.connection().await?;
        let range = self.slice_to_range(offset, limit);
        let members: Vec<String> = conn
            .zrange(self.processing_queue_key(), *range.start(), *range.end())
            .await
            .map_err(Self::map_redis_error)?;
        self.collect_snapshots(&mut conn, &members).await
    }

    pub async fn list_scheduled(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let mut conn = self.connection().await?;
        let range = self.slice_to_range(offset, limit);
        let scheduled_key = self.scheduled_activities_key();
        let queue_entries: Vec<String> = conn
            .zrange_withscores(&scheduled_key, *range.start(), *range.end())
            .await
            .map_err(Self::map_redis_error)?;

        // Scheduled activities are stored in activity_id:activity_json format (same as main queue)
        let mut snapshots = Vec::with_capacity(queue_entries.len());
        for queue_entry in queue_entries {
            // Parse queue entry format: activity_id:activity_json
            if let Some((_, activity_json)) = queue_entry.split_once(':') {
                match serde_json::from_str::<Activity>(activity_json) {
                    Ok(activity) => {
                        // Try to load the snapshot first (which has the latest status)
                        let snapshot = match self.load_snapshot(&mut conn, &activity.id).await? {
                            Some(s) => s,
                            None => ActivitySnapshot::from_activity(&activity),
                        };
                        snapshots.push(snapshot);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to parse scheduled activity JSON");
                    }
                }
            } else {
                tracing::warn!("Invalid queue entry format in scheduled activities: missing colon");
            }
        }

        Ok(snapshots)
    }

    pub async fn list_dead_letter(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, WorkerError> {
        let mut conn = self.connection().await?;
        let range = self.slice_to_range(offset, limit);
        let dead_letter_key = self.dead_letter_queue_key();
        let entries: Vec<String> = conn
            .lrange(&dead_letter_key, *range.start(), *range.end())
            .await
            .map_err(Self::map_redis_error)?;

        let mut records = Vec::with_capacity(entries.len());
        for entry in entries {
            match serde_json::from_str::<DeadLetterEnvelope>(&entry) {
                Ok(envelope) => {
                    let mut snapshot = ActivitySnapshot::from_activity(&envelope.activity);
                    snapshot.update_status(ActivityStatus::DeadLetter, envelope.failed_at);
                    snapshot.set_last_error(Some(envelope.error.clone()), Some(envelope.failed_at));
                    records.push(DeadLetterRecord {
                        activity: snapshot,
                        error: envelope.error,
                        failed_at: envelope.failed_at,
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "queue_inspector: failed to parse dead letter entry");
                }
            }
        }

        Ok(records)
    }

    pub async fn list_completed(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let mut conn = self.connection().await?;
        let completed_key = self.completed_activities_key();

        // Use ZREVRANGE to get completed activities in reverse chronological order (most recent first)
        // This is O(log(N)+M) where N is total items and M is the number we're retrieving
        let range = self.slice_to_range(offset, limit);
        let members: Vec<String> = conn
            .zrevrange(&completed_key, *range.start(), *range.end())
            .await
            .map_err(Self::map_redis_error)?;

        let mut snapshots = Vec::with_capacity(members.len());
        for member in members {
            if let Ok(activity_id) = Uuid::parse_str(&member) {
                if let Ok(Some(snapshot)) = self.load_snapshot(&mut conn, &activity_id).await {
                    snapshots.push(snapshot);
                }
            }
        }

        Ok(snapshots)
    }

    pub async fn get_activity(
        &self,
        activity_id: Uuid,
    ) -> Result<Option<ActivitySnapshot>, WorkerError> {
        let mut conn = self.connection().await?;
        self.load_snapshot(&mut conn, &activity_id).await
    }

    pub async fn get_result(
        &self,
        activity_id: Uuid,
    ) -> Result<Option<serde_json::Value>, WorkerError> {
        let mut conn = self.connection().await?;
        let result_key = format!("result:{}", activity_id);
        let result_json: Option<String> =
            conn.get(&result_key).await.map_err(Self::map_redis_error)?;

        match result_json {
            Some(json) => {
                let value: serde_json::Value = serde_json::from_str(&json).map_err(|e| {
                    WorkerError::QueueError(format!("Failed to parse result: {}", e))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub async fn recent_events(
        &self,
        activity_id: Uuid,
        limit: usize,
    ) -> Result<Vec<ActivityEvent>, WorkerError> {
        let mut conn = self.connection().await?;
        let start = -(limit as isize);
        let raw_events: Vec<String> = conn
            .lrange(self.activity_events_key(&activity_id), start as isize, -1)
            .await
            .map_err(Self::map_redis_error)?;

        let mut events = Vec::with_capacity(raw_events.len());
        for raw in raw_events {
            match serde_json::from_str::<ActivityEvent>(&raw) {
                Ok(event) => events.push(event),
                Err(e) => {
                    tracing::warn!(error = %e, "queue_inspector: failed to parse activity event")
                }
            }
        }

        Ok(events)
    }

    pub async fn stats(&self) -> Result<QueueStats, WorkerError> {
        let mut conn = self.connection().await?;

        let queue_key = self.main_queue_key();
        let processing_key = self.processing_queue_key();

        let total_pending: u64 = conn
            .zcard(&queue_key)
            .await
            .map_err(Self::map_redis_error)?;
        let processing_count: u64 = conn
            .zcard(&processing_key)
            .await
            .map_err(Self::map_redis_error)?;

        // Get count by priority from both pending and processing queues
        let pending_critical: u64 = conn
            .zcount(&queue_key, 4_000_000.0, 4_999_999.0)
            .await
            .map_err(Self::map_redis_error)?;
        let pending_high: u64 = conn
            .zcount(&queue_key, 3_000_000.0, 3_999_999.0)
            .await
            .map_err(Self::map_redis_error)?;
        let pending_normal: u64 = conn
            .zcount(&queue_key, 2_000_000.0, 2_999_999.0)
            .await
            .map_err(Self::map_redis_error)?;
        let pending_low: u64 = conn
            .zcount(&queue_key, 1_000_000.0, 1_999_999.0)
            .await
            .map_err(Self::map_redis_error)?;

        let processing_critical: u64 = conn
            .zcount(&processing_key, 4_000_000.0, 4_999_999.0)
            .await
            .map_err(Self::map_redis_error)?;
        let processing_high: u64 = conn
            .zcount(&processing_key, 3_000_000.0, 3_999_999.0)
            .await
            .map_err(Self::map_redis_error)?;
        let processing_normal: u64 = conn
            .zcount(&processing_key, 2_000_000.0, 2_999_999.0)
            .await
            .map_err(Self::map_redis_error)?;
        let processing_low: u64 = conn
            .zcount(&processing_key, 1_000_000.0, 1_999_999.0)
            .await
            .map_err(Self::map_redis_error)?;

        let scheduled_key = self.scheduled_activities_key();
        let scheduled_count: u64 = conn
            .zcard(&scheduled_key)
            .await
            .map_err(Self::map_redis_error)?;
        let dead_letter_key = self.dead_letter_queue_key();
        let dead_letter_count: u64 = conn
            .llen(&dead_letter_key)
            .await
            .map_err(Self::map_redis_error)?;

        Ok(QueueStats {
            pending_activities: total_pending,
            processing_activities: processing_count,
            critical_priority: pending_critical + processing_critical,
            high_priority: pending_high + processing_high,
            normal_priority: pending_normal + processing_normal,
            low_priority: pending_low + processing_low,
            scheduled_activities: scheduled_count,
            dead_letter_activities: dead_letter_count,
            max_workers: self.max_workers,
        })
    }

    async fn collect_snapshots(
        &self,
        conn: &mut PooledConnection<'_, RedisConnectionManager>,
        members: &[String],
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let mut snapshots = Vec::with_capacity(members.len());
        for member in members {
            if let Some(activity_id) = self.member_to_uuid(member) {
                let snapshot = match self.load_snapshot(conn, &activity_id).await? {
                    Some(s) => s,
                    None => match self.snapshot_from_member(member) {
                        Some(snapshot) => snapshot,
                        None => continue,
                    },
                };
                snapshots.push(snapshot);
            }
        }

        Ok(snapshots)
    }

    async fn load_snapshot(
        &self,
        conn: &mut PooledConnection<'_, RedisConnectionManager>,
        activity_id: &Uuid,
    ) -> Result<Option<ActivitySnapshot>, WorkerError> {
        let key = format!("activity:{}", activity_id);
        let snapshot_json: Option<String> = conn
            .hget(&key, "snapshot")
            .await
            .map_err(Self::map_redis_error)?;
        match snapshot_json {
            Some(value) => serde_json::from_str(&value).map(Some).map_err(|e| {
                WorkerError::QueueError(format!(
                    "Failed to deserialize snapshot for {}: {}",
                    activity_id, e
                ))
            }),
            None => Ok(None),
        }
    }

    fn snapshot_from_member(&self, member: &str) -> Option<ActivitySnapshot> {
        let (_, json) = member.split_once(':')?;
        let activity: Activity = serde_json::from_str(json).ok()?;
        Some(ActivitySnapshot::from_activity(&activity))
    }

    fn member_to_uuid(&self, member: &str) -> Option<Uuid> {
        let (id_str, _) = member.split_once(':')?;
        Uuid::parse_str(id_str).ok()
    }

    async fn connection(
        &self,
    ) -> Result<PooledConnection<'_, RedisConnectionManager>, WorkerError> {
        self.redis_pool
            .get()
            .await
            .map_err(|e| WorkerError::QueueError(format!("Failed to get Redis connection: {}", e)))
    }

    fn main_queue_key(&self) -> String {
        format!("{}:priority_queue", self.queue_name)
    }

    fn processing_queue_key(&self) -> String {
        format!("{}:processing", self.queue_name)
    }

    fn dead_letter_queue_key(&self) -> String {
        format!("{}:dead_letter_queue", self.queue_name)
    }

    fn scheduled_activities_key(&self) -> String {
        format!("{}:scheduled_activities", self.queue_name)
    }

    fn completed_activities_key(&self) -> String {
        format!("{}:completed_activities", self.queue_name)
    }

    fn activity_events_key(&self, activity_id: &Uuid) -> String {
        format!("activity:{}:events", activity_id)
    }

    fn slice_to_range(&self, offset: usize, limit: usize) -> RangeInclusive<isize> {
        let start = offset as isize;
        let end = if limit == 0 {
            start
        } else {
            start + (limit as isize).saturating_sub(1)
        };
        start..=end
    }

    fn map_redis_error(err: redis::RedisError) -> WorkerError {
        WorkerError::QueueError(format!("Redis error: {}", err))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeadLetterEnvelope {
    activity: Activity,
    error: String,
    failed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub activity: ActivitySnapshot,
    pub error: String,
    pub failed_at: DateTime<Utc>,
}
