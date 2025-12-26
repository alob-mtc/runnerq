//! Redis queue operations implementation.

use std::time::Duration;

use bb8_redis::bb8::PooledConnection;
use bb8_redis::RedisConnectionManager;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::RedisBackend;
use crate::activity::activity::ActivityStatus;
use crate::backend::error::BackendError;
use crate::backend::traits::*;
use crate::ActivityPriority;

// Constants
const SNAPSHOT_TTL_SECONDS: i64 = 7 * 24 * 60 * 60; // 7 days
const IDEMPOTENCY_KEY_TTL_SECONDS: u64 = 24 * 60 * 60; // 24 hours
const EVENT_TTL_SECONDS: i64 = 60;
const EVENT_MAX_LEN: isize = 100;
const MAX_RETRY_DELAY_SECONDS: u64 = 60 * 60; // 1 hour
const RESULT_TTL_SECONDS: u64 = 86400; // 24 hours

// ============================================================================
// Internal helpers
// ============================================================================

fn now() -> DateTime<Utc> {
    Utc::now()
}

async fn get_conn(
    backend: &RedisBackend,
) -> Result<PooledConnection<'_, RedisConnectionManager>, BackendError> {
    backend
        .pool()
        .get()
        .await
        .map_err(|e| BackendError::Unavailable(format!("Failed to get Redis connection: {}", e)))
}

fn calculate_priority_score(priority: &ActivityPriority, created_at: DateTime<Utc>) -> f64 {
    let priority_weight = match priority {
        ActivityPriority::Critical => 4_000_000.0,
        ActivityPriority::High => 3_000_000.0,
        ActivityPriority::Normal => 2_000_000.0,
        ActivityPriority::Low => 1_000_000.0,
    };
    let timestamp_micros = created_at.timestamp_micros() as f64 / 1_000_000.0;
    priority_weight + (timestamp_micros % 1_000_000.0)
}

fn create_queue_entry(activity: &QueuedActivity) -> String {
    format!(
        "{}:{}",
        activity.id,
        serde_json::to_string(activity).unwrap_or_default()
    )
}

fn parse_queue_entry(entry: &str) -> Result<QueuedActivity, BackendError> {
    if let Some(colon_pos) = entry.find(':') {
        let json = &entry[colon_pos + 1..];
        serde_json::from_str(json)
            .map_err(|e| BackendError::Internal(format!("Failed to parse queue entry: {}", e)))
    } else {
        Err(BackendError::Internal("Invalid queue entry format".into()))
    }
}

// ============================================================================
// Snapshot management
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct InternalSnapshot {
    pub id: Uuid,
    pub activity_type: String,
    pub payload: Value,
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
    pub metadata: std::collections::HashMap<String, String>,
    pub last_error: Option<String>,
    pub last_error_at: Option<DateTime<Utc>>,
    pub status_updated_at: DateTime<Utc>,
    pub score: Option<f64>,
    pub lease_deadline_ms: Option<i64>,
    pub processing_member: Option<String>,
    pub idempotency_key: Option<String>,
}

impl InternalSnapshot {
    fn from_queued(activity: &QueuedActivity) -> Self {
        Self {
            id: activity.id,
            activity_type: activity.activity_type.clone(),
            payload: activity.payload.clone(),
            priority: activity.priority.clone(),
            status: ActivityStatus::Pending,
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
            status_updated_at: now(),
            score: None,
            lease_deadline_ms: None,
            processing_member: None,
            idempotency_key: activity.idempotency_key.as_ref().map(|(k, _)| k.clone()),
        }
    }

    fn to_public(&self) -> ActivitySnapshot {
        ActivitySnapshot {
            id: self.id,
            activity_type: self.activity_type.clone(),
            payload: self.payload.clone(),
            priority: self.priority.clone(),
            status: self.status.clone(),
            created_at: self.created_at,
            scheduled_at: self.scheduled_at,
            started_at: self.started_at,
            completed_at: self.completed_at,
            current_worker_id: self.current_worker_id.clone(),
            last_worker_id: self.last_worker_id.clone(),
            retry_count: self.retry_count,
            max_retries: self.max_retries,
            timeout_seconds: self.timeout_seconds,
            retry_delay_seconds: self.retry_delay_seconds,
            metadata: self.metadata.clone(),
            last_error: self.last_error.clone(),
            last_error_at: self.last_error_at,
            status_updated_at: self.status_updated_at,
            score: self.score,
            lease_deadline_ms: self.lease_deadline_ms,
            processing_member: self.processing_member.clone(),
            idempotency_key: self.idempotency_key.clone(),
        }
    }
}

async fn write_snapshot(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    backend: &RedisBackend,
    snapshot: &InternalSnapshot,
) -> Result<(), BackendError> {
    let activity_key = RedisBackend::activity_key(&snapshot.id);
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
    fields.push((
        "idempotency_key".to_string(),
        snapshot.idempotency_key.clone().unwrap_or_default(),
    ));

    let _: () = conn.hset_multiple(&activity_key, &fields).await?;
    let _: () = conn.expire(&activity_key, SNAPSHOT_TTL_SECONDS).await?;
    let _ = backend; // silence unused warning
    Ok(())
}

pub(super) async fn load_snapshot(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    activity_id: &Uuid,
) -> Result<Option<InternalSnapshot>, BackendError> {
    let activity_key = RedisBackend::activity_key(activity_id);
    let snapshot_json: Option<String> = conn.hget(&activity_key, "snapshot").await?;
    match snapshot_json {
        Some(json) => {
            let snapshot: InternalSnapshot = serde_json::from_str(&json).map_err(|e| {
                BackendError::Internal(format!("Failed to deserialize snapshot: {}", e))
            })?;
            Ok(Some(snapshot))
        }
        None => Ok(None),
    }
}

async fn record_event(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    backend: &RedisBackend,
    activity_id: &Uuid,
    event_type: ActivityEventType,
    worker_id: Option<&str>,
    detail: Option<Value>,
) -> Result<(), BackendError> {
    let event = ActivityEvent {
        activity_id: *activity_id,
        timestamp: now(),
        event_type,
        worker_id: worker_id.map(|s| s.to_string()),
        detail,
    };

    let payload = serde_json::to_string(&event)?;

    // Write to global Redis Stream
    let global_stream_key = backend.events_stream_key();
    let _: Option<String> = conn
        .xadd(&global_stream_key, "*", &[("event", payload.as_str())])
        .await?;

    // Trim stream
    let _: i64 = redis::cmd("XTRIM")
        .arg(&global_stream_key)
        .arg("MAXLEN")
        .arg("~")
        .arg(EVENT_MAX_LEN)
        .query_async(&mut **conn)
        .await?;
    let _: () = conn.expire(&global_stream_key, EVENT_TTL_SECONDS).await?;

    // Per-activity event list
    let events_key = RedisBackend::activity_events_key(activity_id);
    let _: () = conn.rpush(&events_key, payload).await?;
    let _: () = conn.ltrim(&events_key, -EVENT_MAX_LEN, -1).await?;
    let _: () = conn.expire(&events_key, SNAPSHOT_TTL_SECONDS).await?;

    Ok(())
}

async fn add_to_completed_set(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    backend: &RedisBackend,
    activity_id: &Uuid,
    completed_at: DateTime<Utc>,
) -> Result<(), BackendError> {
    let completed_key = backend.completed_key();
    let score = completed_at.timestamp();
    let member = activity_id.to_string();

    let _: () = conn.zadd(&completed_key, &member, score).await?;

    let ttl: i64 = conn.ttl(&completed_key).await?;
    if ttl == -1 {
        let _: () = conn.expire(&completed_key, SNAPSHOT_TTL_SECONDS).await?;
    }

    Ok(())
}

async fn ack_processing(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    backend: &RedisBackend,
    activity_id: &Uuid,
) -> Result<(), BackendError> {
    let processing_key = backend.processing_queue_key();
    let activity_key = RedisBackend::activity_key(activity_id);

    let member: Option<String> = conn.hget(&activity_key, "processing_member").await?;

    if let Some(m) = member {
        let removed: i32 = conn.zrem(&processing_key, &m).await?;
        if removed == 0 {
            warn!(activity_id = %activity_id, "ack_processing: member not found in processing ZSET");
        }
        let _: () = conn
            .hdel(&activity_key, &["processing_member", "lease_deadline_ms"])
            .await?;
    }

    Ok(())
}

// ============================================================================
// Queue Operations
// ============================================================================

pub async fn enqueue(backend: &RedisBackend, activity: QueuedActivity) -> Result<(), BackendError> {
    let mut conn = get_conn(backend).await?;

    // Handle scheduled activities
    if let Some(scheduled_at) = activity.scheduled_at {
        let activity_json = serde_json::to_string(&activity)?;
        let scheduled_key = backend.scheduled_key();
        let score = scheduled_at.timestamp();
        let _: () = conn.zadd(&scheduled_key, activity_json, score).await?;

        let mut snapshot = InternalSnapshot::from_queued(&activity);
        snapshot.status = ActivityStatus::Pending;
        snapshot.scheduled_at = Some(scheduled_at);
        snapshot.status_updated_at = now();
        write_snapshot(&mut conn, backend, &snapshot).await?;

        record_event(
            &mut conn,
            backend,
            &activity.id,
            ActivityEventType::Scheduled,
            None,
            Some(json!({ "scheduled_at": scheduled_at })),
        )
        .await?;

        debug!(activity_id = %activity.id, scheduled_at = %scheduled_at, "Activity scheduled");
        return Ok(());
    }

    // Immediate enqueue
    let queue_key = backend.main_queue_key();
    let queue_entry = create_queue_entry(&activity);
    let score = calculate_priority_score(&activity.priority, activity.created_at);

    let _: () = conn.zadd(&queue_key, queue_entry, score).await?;

    let mut snapshot = InternalSnapshot::from_queued(&activity);
    snapshot.score = Some(score);
    snapshot.status_updated_at = now();
    write_snapshot(&mut conn, backend, &snapshot).await?;

    record_event(
        &mut conn,
        backend,
        &activity.id,
        ActivityEventType::Enqueued,
        None,
        Some(json!({
            "priority": activity.priority,
            "scheduled_at": activity.scheduled_at,
        })),
    )
    .await?;

    info!(
        activity_id = %activity.id,
        activity_type = ?activity.activity_type,
        priority = ?activity.priority,
        score = %score,
        "Activity enqueued"
    );
    Ok(())
}

pub async fn dequeue(
    backend: &RedisBackend,
    worker_id: &str,
    timeout: Duration,
) -> Result<Option<DequeuedActivity>, BackendError> {
    let mut conn = get_conn(backend).await?;

    let queue_key = backend.main_queue_key();
    let processing_key = backend.processing_queue_key();
    let lease_ms = backend.default_lease_ms();

    let start_time = std::time::Instant::now();
    let mut sleep_duration = Duration::from_millis(10);

    while start_time.elapsed() < timeout {
        let now_ms = Utc::now().timestamp_millis() as u64;

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
            let activity = parse_queue_entry(&queue_entry)?;
            let lease_deadline_ms = (now_ms + lease_ms) as i64;
            let lease_deadline =
                DateTime::from_timestamp_millis(lease_deadline_ms).unwrap_or_else(Utc::now);

            let activity_key = RedisBackend::activity_key(&activity.id);
            let meta_fields = vec![
                ("processing_member".to_string(), queue_entry.clone()),
                (
                    "lease_deadline_ms".to_string(),
                    lease_deadline_ms.to_string(),
                ),
            ];
            let _: () = conn.hset_multiple(&activity_key, &meta_fields).await?;

            let mut snapshot = load_snapshot(&mut conn, &activity.id)
                .await?
                .unwrap_or_else(|| InternalSnapshot::from_queued(&activity));

            let started_at = now();
            snapshot.status = ActivityStatus::Running;
            snapshot.status_updated_at = started_at;
            snapshot.started_at = Some(started_at);
            snapshot.lease_deadline_ms = Some(lease_deadline_ms);
            snapshot.processing_member = Some(queue_entry.clone());
            snapshot.current_worker_id = Some(worker_id.to_string());
            snapshot.last_worker_id = Some(worker_id.to_string());
            snapshot.score = Some(calculate_priority_score(
                &activity.priority,
                activity.created_at,
            ));

            write_snapshot(&mut conn, backend, &snapshot).await?;

            record_event(
                &mut conn,
                backend,
                &activity.id,
                ActivityEventType::Dequeued,
                Some(worker_id),
                Some(json!({ "lease_deadline_ms": lease_deadline_ms })),
            )
            .await?;

            // Extend lease if needed
            if lease_ms / 1000 < activity.timeout_seconds {
                let delta = activity.timeout_seconds - (lease_ms / 1000);
                if delta > 0 {
                    let lease_buffer = 10;
                    let _ = extend_lease(
                        backend,
                        activity.id,
                        Duration::from_secs(delta + lease_buffer),
                    )
                    .await;
                }
            }

            debug!(
                activity_id = %activity.id,
                activity_type = ?activity.activity_type,
                priority = ?activity.priority,
                "Activity claimed"
            );

            return Ok(Some(DequeuedActivity {
                activity,
                lease_id: queue_entry,
                attempt: snapshot.retry_count + 1,
                lease_deadline,
            }));
        }

        tokio::time::sleep(sleep_duration).await;
        sleep_duration = std::cmp::min(sleep_duration * 2, Duration::from_millis(1000));
    }

    Ok(None)
}

pub async fn ack_success(
    backend: &RedisBackend,
    activity_id: Uuid,
    _lease_id: &str,
    result: Option<Value>,
    worker_id: &str,
) -> Result<(), BackendError> {
    let mut conn = get_conn(backend).await?;

    ack_processing(&mut conn, backend, &activity_id).await?;

    let mut snapshot = load_snapshot(&mut conn, &activity_id)
        .await?
        .ok_or_else(|| BackendError::NotFound(format!("Activity {} not found", activity_id)))?;

    let completed_at = now();
    snapshot.status = ActivityStatus::Completed;
    snapshot.status_updated_at = completed_at;
    snapshot.completed_at = Some(completed_at);
    snapshot.last_worker_id = Some(worker_id.to_string());
    snapshot.current_worker_id = None;
    snapshot.processing_member = None;
    snapshot.lease_deadline_ms = None;
    snapshot.last_error = None;

    write_snapshot(&mut conn, backend, &snapshot).await?;
    add_to_completed_set(&mut conn, backend, &activity_id, completed_at).await?;

    record_event(
        &mut conn,
        backend,
        &activity_id,
        ActivityEventType::Completed,
        Some(worker_id),
        Some(json!({ "activity_type": snapshot.activity_type })),
    )
    .await?;

    // Store result
    if let Some(data) = result {
        let activity_result = ActivityResult {
            data: Some(data),
            state: ResultState::Ok,
        };
        store_result(backend, activity_id, activity_result).await?;
    }

    info!(activity_id = %activity_id, "Activity completed");
    Ok(())
}

pub async fn ack_failure(
    backend: &RedisBackend,
    activity_id: Uuid,
    _lease_id: &str,
    failure: FailureKind,
    worker_id: &str,
) -> Result<bool, BackendError> {
    let mut conn = get_conn(backend).await?;

    let mut snapshot = load_snapshot(&mut conn, &activity_id)
        .await?
        .ok_or_else(|| BackendError::NotFound(format!("Activity {} not found", activity_id)))?;

    let now_ts = now();

    snapshot.last_worker_id = Some(worker_id.to_string());

    snapshot.current_worker_id = None;
    snapshot.processing_member = None;
    snapshot.lease_deadline_ms = None;

    let (error_message, retryable) = match &failure {
        FailureKind::Retryable { reason } => (reason.clone(), true),
        FailureKind::NonRetryable { reason } => (reason.clone(), false),
        FailureKind::Timeout => ("Activity execution timed out".to_string(), true),
    };

    snapshot.last_error = Some(error_message.clone());
    snapshot.last_error_at = Some(now_ts);

    if !retryable {
        ack_processing(&mut conn, backend, &activity_id).await?;
        snapshot.status = ActivityStatus::Failed;
        snapshot.status_updated_at = now_ts;
        snapshot.completed_at = Some(now_ts);
        write_snapshot(&mut conn, backend, &snapshot).await?;
        add_to_completed_set(&mut conn, backend, &activity_id, now_ts).await?;
        record_event(
            &mut conn,
            backend,
            &activity_id,
            ActivityEventType::Failed,
            Some(worker_id),
            Some(json!({ "retryable": false, "error": error_message })),
        )
        .await?;
        return Ok(false);
    }

    // Check if we can retry
    if snapshot.max_retries == 0 || snapshot.retry_count + 1 < snapshot.max_retries {
        snapshot.retry_count += 1;
        snapshot.status = ActivityStatus::Retrying;
        snapshot.status_updated_at = now_ts;

        // Calculate backoff delay
        let backoff_multiplier = 2_u64.saturating_pow(snapshot.retry_count);
        let delay_seconds = snapshot
            .retry_delay_seconds
            .saturating_mul(backoff_multiplier)
            .min(MAX_RETRY_DELAY_SECONDS);
        let scheduled_at = Utc::now() + chrono::Duration::seconds(delay_seconds as i64);
        snapshot.scheduled_at = Some(scheduled_at);

        ack_processing(&mut conn, backend, &activity_id).await?;
        write_snapshot(&mut conn, backend, &snapshot).await?;

        record_event(
            &mut conn,
            backend,
            &activity_id,
            ActivityEventType::Retrying,
            Some(worker_id),
            Some(json!({
                "retry_count": snapshot.retry_count,
                "scheduled_at": scheduled_at,
                "error": error_message,
            })),
        )
        .await?;

        // Re-enqueue as scheduled
        let retry_activity = QueuedActivity {
            id: activity_id,
            activity_type: snapshot.activity_type.clone(),
            payload: snapshot.payload.clone(),
            priority: snapshot.priority.clone(),
            max_retries: snapshot.max_retries,
            retry_count: snapshot.retry_count,
            timeout_seconds: snapshot.timeout_seconds,
            retry_delay_seconds: snapshot.retry_delay_seconds,
            scheduled_at: Some(scheduled_at),
            metadata: snapshot.metadata.clone(),
            idempotency_key: snapshot
                .idempotency_key
                .as_ref()
                .map(|k| (k.clone(), IdempotencyBehavior::AllowReuse)),
            created_at: snapshot.created_at,
        };

        let activity_json = serde_json::to_string(&retry_activity)?;
        let scheduled_key = backend.scheduled_key();
        let _: () = conn
            .zadd(&scheduled_key, activity_json, scheduled_at.timestamp())
            .await?;

        info!(activity_id = %activity_id, retry_count = snapshot.retry_count, "Activity scheduled for retry");
        return Ok(false);
    }

    // Move to dead letter queue
    ack_processing(&mut conn, backend, &activity_id).await?;
    snapshot.status = ActivityStatus::DeadLetter;
    snapshot.status_updated_at = now_ts;
    snapshot.completed_at = Some(now_ts);
    write_snapshot(&mut conn, backend, &snapshot).await?;
    add_to_completed_set(&mut conn, backend, &activity_id, now_ts).await?;

    let dead_letter_entry = json!({
        "activity": {
            "id": snapshot.id,
            "activity_type": snapshot.activity_type,
            "payload": snapshot.payload,
            "priority": snapshot.priority,
            "retry_count": snapshot.retry_count,
            "max_retries": snapshot.max_retries,
            "timeout_seconds": snapshot.timeout_seconds,
            "retry_delay_seconds": snapshot.retry_delay_seconds,
            "created_at": snapshot.created_at,
            "metadata": snapshot.metadata,
        },
        "error": error_message,
        "failed_at": now_ts
    });

    let dead_letter_key = backend.dead_letter_key();
    let _: () = conn
        .rpush(&dead_letter_key, serde_json::to_string(&dead_letter_entry)?)
        .await?;

    record_event(
        &mut conn,
        backend,
        &activity_id,
        ActivityEventType::DeadLetter,
        Some(worker_id),
        Some(json!({ "error": error_message })),
    )
    .await?;

    error!(activity_id = %activity_id, "Activity moved to dead letter queue");
    Ok(true)
}

pub async fn process_scheduled(backend: &RedisBackend) -> Result<u64, BackendError> {
    let mut conn = get_conn(backend).await?;

    let now_ts = Utc::now().timestamp();
    let scheduled_key = backend.scheduled_key();

    let activity_jsons: Vec<String> = conn
        .zrangebyscore_limit(&scheduled_key, 0, now_ts, 0, 100)
        .await?;

    let mut count = 0u64;
    for activity_json in activity_jsons {
        let _: () = conn.zrem(&scheduled_key, &activity_json).await?;

        match serde_json::from_str::<QueuedActivity>(&activity_json) {
            Ok(mut activity) => {
                activity.scheduled_at = None; // Clear scheduled_at for immediate processing
                enqueue(backend, activity).await?;
                count += 1;
            }
            Err(e) => {
                error!(error = %e, "Failed to parse scheduled activity");
            }
        }
    }

    if count > 0 {
        info!(count = count, "Processed scheduled activities");
    }

    Ok(count)
}

pub async fn requeue_expired(
    backend: &RedisBackend,
    batch_size: usize,
) -> Result<u64, BackendError> {
    let mut conn = get_conn(backend).await?;

    let processing_key = backend.processing_queue_key();
    let main_key = backend.main_queue_key();
    let now_ms = Utc::now().timestamp_millis();

    let lua = r#"
    local now = tonumber(ARGV[1])
    local limit = tonumber(ARGV[2])
    local moved_ids = {}
    local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)

    for _, m in ipairs(members) do
        if redis.call('ZREM', KEYS[1], m) == 1 then
            local existing_score = redis.call('ZSCORE', KEYS[2], m)
            if existing_score == false then
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
    end
    return moved_ids
    "#;

    let moved_ids: Vec<String> = redis::cmd("EVAL")
        .arg(lua)
        .arg(2)
        .arg(&processing_key)
        .arg(&main_key)
        .arg(now_ms)
        .arg(batch_size as i64)
        .query_async(&mut *conn)
        .await?;

    for id_str in &moved_ids {
        if let Ok(activity_id) = Uuid::parse_str(id_str) {
            if let Ok(Some(mut snapshot)) = load_snapshot(&mut conn, &activity_id).await {
                snapshot.status = ActivityStatus::Pending;
                snapshot.status_updated_at = now();
                snapshot.current_worker_id = None;
                snapshot.processing_member = None;
                snapshot.lease_deadline_ms = None;
                let _ = write_snapshot(&mut conn, backend, &snapshot).await;
                let _ = record_event(
                    &mut conn,
                    backend,
                    &activity_id,
                    ActivityEventType::Requeued,
                    None,
                    Some(json!({ "reason": "lease_expired" })),
                )
                .await;
            }
        }
    }

    Ok(moved_ids.len() as u64)
}

pub async fn extend_lease(
    backend: &RedisBackend,
    activity_id: Uuid,
    extend_by: Duration,
) -> Result<bool, BackendError> {
    let mut conn = get_conn(backend).await?;

    let processing_key = backend.processing_queue_key();
    let activity_key = RedisBackend::activity_key(&activity_id);

    let member: Option<String> = conn.hget(&activity_key, "processing_member").await?;
    if member.is_none() {
        return Ok(false);
    }
    let member = member.unwrap();

    let extend_ms = extend_by.as_millis() as i64;

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
        .await?;

    if new_deadline > 0 {
        if let Ok(Some(mut snapshot)) = load_snapshot(&mut conn, &activity_id).await {
            snapshot.lease_deadline_ms = Some(new_deadline);
            let _ = write_snapshot(&mut conn, backend, &snapshot).await;
            let _ = record_event(
                &mut conn,
                backend,
                &activity_id,
                ActivityEventType::LeaseExtended,
                None,
                Some(json!({
                    "new_deadline_ms": new_deadline,
                    "extend_by_ms": extend_ms,
                })),
            )
            .await;
        }
    }

    debug!(
        activity_id = %activity_id,
        new_deadline_ms = %new_deadline,
        "Lease extended"
    );
    Ok(new_deadline > 0)
}

pub async fn store_result(
    backend: &RedisBackend,
    activity_id: Uuid,
    result: ActivityResult,
) -> Result<(), BackendError> {
    let mut conn = get_conn(backend).await?;

    let result_key = RedisBackend::result_key(&activity_id);
    let result_value = serde_json::to_value(&result)?;
    let result_json = result_value.to_string();

    let _: () = conn
        .set_ex(&result_key, result_json, RESULT_TTL_SECONDS)
        .await?;

    record_event(
        &mut conn,
        backend,
        &activity_id,
        ActivityEventType::ResultStored,
        None,
        Some(result_value),
    )
    .await?;

    info!(activity_id = %activity_id, "Activity result stored");
    Ok(())
}

pub async fn get_result(
    backend: &RedisBackend,
    activity_id: Uuid,
) -> Result<Option<ActivityResult>, BackendError> {
    let mut conn = get_conn(backend).await?;

    let result_key = RedisBackend::result_key(&activity_id);
    let result_json: Option<String> = conn.get(&result_key).await?;

    match result_json {
        Some(json) => {
            let result: ActivityResult = serde_json::from_str(&json)?;
            Ok(Some(result))
        }
        None => Ok(None),
    }
}

pub async fn check_idempotency(
    backend: &RedisBackend,
    activity: &QueuedActivity,
) -> Result<Option<Uuid>, BackendError> {
    let Some((ref key, ref behavior)) = activity.idempotency_key else {
        return Ok(None);
    };

    let mut conn = get_conn(backend).await?;
    let idempotency_key = backend.idempotency_key(key);

    // Try to atomically claim the idempotency key
    let result: Option<String> = redis::cmd("SET")
        .arg(&idempotency_key)
        .arg(activity.id.to_string())
        .arg("NX")
        .arg("EX")
        .arg(IDEMPOTENCY_KEY_TTL_SECONDS)
        .query_async(&mut *conn)
        .await?;

    if result.is_some() {
        // We successfully claimed the key
        return Ok(None);
    }

    // Key already exists, get the existing activity ID
    let existing_id_str: Option<String> = conn.get(&idempotency_key).await?;
    let existing_id = existing_id_str
        .and_then(|s| Uuid::parse_str(&s).ok())
        .ok_or_else(|| {
            BackendError::Internal("Idempotency key exists but activity_id not found".to_string())
        })?;

    // Load existing snapshot to check status
    let existing_snapshot = match load_snapshot(&mut conn, &existing_id).await? {
        Some(s) => s,
        None => {
            // Snapshot doesn't exist, claim the key
            let _: () = conn
                .set_ex(
                    &idempotency_key,
                    activity.id.to_string(),
                    IDEMPOTENCY_KEY_TTL_SECONDS,
                )
                .await?;
            return Ok(None);
        }
    };

    match behavior {
        IdempotencyBehavior::AllowReuse => {
            let _: () = conn
                .set_ex(
                    &idempotency_key,
                    activity.id.to_string(),
                    IDEMPOTENCY_KEY_TTL_SECONDS,
                )
                .await?;
            Ok(None)
        }
        IdempotencyBehavior::ReturnExisting => Ok(Some(existing_id)),
        IdempotencyBehavior::AllowReuseOnFailure => {
            if existing_snapshot.status != ActivityStatus::Failed
                && existing_snapshot.status != ActivityStatus::DeadLetter
            {
                return Err(BackendError::IdempotencyConflict(format!(
                    "Idempotency key '{}' exists with status '{:?}'",
                    key, existing_snapshot.status
                )));
            }
            // Atomically replace
            let lua = r#"
            local current = redis.call('GET', KEYS[1])
            if current == ARGV[1] then
                redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
                return 1
            else
                return 0
            end
            "#;
            let claimed: i64 = redis::cmd("EVAL")
                .arg(lua)
                .arg(1)
                .arg(&idempotency_key)
                .arg(existing_id.to_string())
                .arg(activity.id.to_string())
                .arg(IDEMPOTENCY_KEY_TTL_SECONDS)
                .query_async(&mut *conn)
                .await?;
            if claimed == 1 {
                Ok(None)
            } else {
                Err(BackendError::IdempotencyConflict(format!(
                    "Idempotency key '{}' was claimed by another request",
                    key
                )))
            }
        }
        IdempotencyBehavior::NoReuse => Err(BackendError::DuplicateActivity(format!(
            "Idempotency key '{}' already exists for activity {}",
            key, existing_id
        ))),
    }
}

// Helper to convert internal snapshot to public
pub(super) fn snapshot_to_public(snapshot: &InternalSnapshot) -> ActivitySnapshot {
    snapshot.to_public()
}
