//! Redis inspection operations implementation.

use std::time::Duration;

use bb8_redis::bb8::PooledConnection;
use bb8_redis::RedisConnectionManager;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, StreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};
use uuid::Uuid;

use super::queue::{load_snapshot, snapshot_to_public};
use super::RedisBackend;
use crate::activity::activity::ActivityStatus;
use crate::storage::error::StorageError;
use crate::storage::traits::*;

async fn get_conn(
    backend: &RedisBackend,
) -> Result<PooledConnection<'_, RedisConnectionManager>, StorageError> {
    backend
        .pool()
        .get()
        .await
        .map_err(|e| StorageError::Unavailable(format!("Failed to get Redis connection: {}", e)))
}

fn slice_to_range(offset: usize, limit: usize) -> std::ops::RangeInclusive<isize> {
    let start = offset as isize;
    let end = if limit == 0 {
        start
    } else {
        start + (limit as isize).saturating_sub(1)
    };
    start..=end
}

async fn collect_snapshots(
    conn: &mut PooledConnection<'_, RedisConnectionManager>,
    members: &[String],
) -> Result<Vec<ActivitySnapshot>, StorageError> {
    let mut snapshots = Vec::with_capacity(members.len());
    for member in members {
        if let Some(activity_id) = member_to_uuid(member) {
            let snapshot = match load_snapshot(conn, &activity_id).await? {
                Some(s) => snapshot_to_public(&s),
                None => match snapshot_from_member(member) {
                    Some(snapshot) => snapshot,
                    None => continue,
                },
            };
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

fn snapshot_from_member(member: &str) -> Option<ActivitySnapshot> {
    let (_, json) = member.split_once(':')?;
    let activity: QueuedActivity = serde_json::from_str(json).ok()?;
    Some(queued_to_snapshot(&activity))
}

fn member_to_uuid(member: &str) -> Option<Uuid> {
    let (id_str, _) = member.split_once(':')?;
    Uuid::parse_str(id_str).ok()
}

fn queued_to_snapshot(activity: &QueuedActivity) -> ActivitySnapshot {
    ActivitySnapshot {
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
        status_updated_at: chrono::Utc::now(),
        score: None,
        lease_deadline_ms: None,
        processing_member: None,
        idempotency_key: activity.idempotency_key.as_ref().map(|(k, _)| k.clone()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeadLetterEnvelope {
    activity: serde_json::Value,
    error: String,
    failed_at: DateTime<Utc>,
}

// ============================================================================
// Inspection Operations
// ============================================================================

pub async fn stats(backend: &RedisBackend) -> Result<QueueStats, StorageError> {
    let mut conn = get_conn(backend).await?;

    let queue_key = backend.main_queue_key();
    let processing_key = backend.processing_queue_key();
    let scheduled_key = backend.scheduled_key();
    let dead_letter_key = backend.dead_letter_key();

    let total_pending: u64 = conn.zcard(&queue_key).await?;
    let processing_count: u64 = conn.zcard(&processing_key).await?;

    // Get count by priority
    let pending_critical: u64 = conn.zcount(&queue_key, 4_000_000.0, 4_999_999.0).await?;
    let pending_high: u64 = conn.zcount(&queue_key, 3_000_000.0, 3_999_999.0).await?;
    let pending_normal: u64 = conn.zcount(&queue_key, 2_000_000.0, 2_999_999.0).await?;
    let pending_low: u64 = conn.zcount(&queue_key, 1_000_000.0, 1_999_999.0).await?;

    let processing_critical: u64 = conn
        .zcount(&processing_key, 4_000_000.0, 4_999_999.0)
        .await?;
    let processing_high: u64 = conn
        .zcount(&processing_key, 3_000_000.0, 3_999_999.0)
        .await?;
    let processing_normal: u64 = conn
        .zcount(&processing_key, 2_000_000.0, 2_999_999.0)
        .await?;
    let processing_low: u64 = conn
        .zcount(&processing_key, 1_000_000.0, 1_999_999.0)
        .await?;

    let scheduled_count: u64 = conn.zcard(&scheduled_key).await?;
    let dead_letter_count: u64 = conn.llen(&dead_letter_key).await?;

    Ok(QueueStats {
        pending: total_pending,
        processing: processing_count,
        scheduled: scheduled_count,
        dead_letter: dead_letter_count,
        by_priority: PriorityBreakdown {
            critical: pending_critical + processing_critical,
            high: pending_high + processing_high,
            normal: pending_normal + processing_normal,
            low: pending_low + processing_low,
        },
        max_workers: None,
    })
}

pub async fn list_pending(
    backend: &RedisBackend,
    offset: usize,
    limit: usize,
) -> Result<Vec<ActivitySnapshot>, StorageError> {
    let mut conn = get_conn(backend).await?;
    let range = slice_to_range(offset, limit);
    let members: Vec<String> = conn
        .zrevrange(backend.main_queue_key(), *range.start(), *range.end())
        .await?;
    collect_snapshots(&mut conn, &members).await
}

pub async fn list_processing(
    backend: &RedisBackend,
    offset: usize,
    limit: usize,
) -> Result<Vec<ActivitySnapshot>, StorageError> {
    let mut conn = get_conn(backend).await?;
    let range = slice_to_range(offset, limit);
    let members: Vec<String> = conn
        .zrange(backend.processing_queue_key(), *range.start(), *range.end())
        .await?;
    collect_snapshots(&mut conn, &members).await
}

pub async fn list_scheduled(
    backend: &RedisBackend,
    offset: usize,
    limit: usize,
) -> Result<Vec<ActivitySnapshot>, StorageError> {
    let mut conn = get_conn(backend).await?;
    let range = slice_to_range(offset, limit);
    // Scheduled entries are stored in format "uuid:json"
    let entries: Vec<String> = conn
        .zrange(backend.scheduled_key(), *range.start(), *range.end())
        .await?;

    let mut snapshots = Vec::with_capacity(entries.len());
    for entry in entries {
        // Parse the entry format: "uuid:json"
        let json_part = match entry.split_once(':') {
            Some((_, json)) => json,
            None => {
                warn!(entry = %entry, "Scheduled entry missing colon separator");
                continue;
            }
        };

        match serde_json::from_str::<QueuedActivity>(json_part) {
            Ok(activity) => {
                let snapshot = match load_snapshot(&mut conn, &activity.id).await? {
                    Some(s) => snapshot_to_public(&s),
                    None => queued_to_snapshot(&activity),
                };
                snapshots.push(snapshot);
            }
            Err(e) => {
                warn!(error = %e, json = %json_part, "Failed to parse scheduled activity JSON");
            }
        }
    }

    Ok(snapshots)
}

pub async fn list_completed(
    backend: &RedisBackend,
    offset: usize,
    limit: usize,
) -> Result<Vec<ActivitySnapshot>, StorageError> {
    let mut conn = get_conn(backend).await?;
    let completed_key = backend.completed_key();
    let range = slice_to_range(offset, limit);
    let members: Vec<String> = conn
        .zrevrange(&completed_key, *range.start(), *range.end())
        .await?;

    let mut snapshots = Vec::with_capacity(members.len());
    for member in members {
        if let Ok(activity_id) = Uuid::parse_str(&member) {
            if let Ok(Some(snapshot)) = load_snapshot(&mut conn, &activity_id).await {
                snapshots.push(snapshot_to_public(&snapshot));
            }
        }
    }

    Ok(snapshots)
}

pub async fn list_dead_letter(
    backend: &RedisBackend,
    offset: usize,
    limit: usize,
) -> Result<Vec<DeadLetterRecord>, StorageError> {
    let mut conn = get_conn(backend).await?;
    let range = slice_to_range(offset, limit);
    let dead_letter_key = backend.dead_letter_key();
    let entries: Vec<String> = conn
        .lrange(&dead_letter_key, *range.start(), *range.end())
        .await?;

    let mut records = Vec::with_capacity(entries.len());
    for entry in entries {
        match serde_json::from_str::<DeadLetterEnvelope>(&entry) {
            Ok(envelope) => {
                // Parse the activity from JSON value
                let activity: QueuedActivity =
                    match serde_json::from_value(envelope.activity.clone()) {
                        Ok(a) => a,
                        Err(_) => continue,
                    };
                let mut snapshot = queued_to_snapshot(&activity);
                snapshot.status = ActivityStatus::DeadLetter;
                snapshot.status_updated_at = envelope.failed_at;
                snapshot.last_error = Some(envelope.error.clone());
                snapshot.last_error_at = Some(envelope.failed_at);
                records.push(DeadLetterRecord {
                    activity: snapshot,
                    error: envelope.error,
                    failed_at: envelope.failed_at,
                });
            }
            Err(e) => {
                warn!(error = %e, "Failed to parse dead letter entry");
            }
        }
    }

    Ok(records)
}

pub async fn get_activity(
    backend: &RedisBackend,
    activity_id: Uuid,
) -> Result<Option<ActivitySnapshot>, StorageError> {
    let mut conn = get_conn(backend).await?;
    match load_snapshot(&mut conn, &activity_id).await? {
        Some(s) => Ok(Some(snapshot_to_public(&s))),
        None => Ok(None),
    }
}

pub async fn get_activity_events(
    backend: &RedisBackend,
    activity_id: Uuid,
    limit: usize,
) -> Result<Vec<ActivityEvent>, StorageError> {
    let mut conn = get_conn(backend).await?;
    let events_key = RedisBackend::activity_events_key(&activity_id);
    let start = -(limit as isize);
    let raw_events: Vec<String> = conn.lrange(&events_key, start, -1).await?;

    let mut events = Vec::with_capacity(raw_events.len());
    for raw in raw_events {
        match serde_json::from_str::<ActivityEvent>(&raw) {
            Ok(event) => events.push(event),
            Err(e) => {
                warn!(error = %e, "Failed to parse activity event");
            }
        }
    }

    Ok(events)
}

pub fn event_stream(
    backend: &RedisBackend,
) -> BoxStream<'static, Result<ActivityEvent, StorageError>> {
    let redis_pool = backend.pool().clone();
    let stream_key = backend.events_stream_key();
    let queue_name = backend.queue_name().to_string();

    let stream = futures::stream::unfold(
        (redis_pool, stream_key, queue_name, "$".to_string()),
        move |state| async move {
            let (pool, key, qn, mut last_id) = state;

            let mut conn = match pool.get().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(
                        queue_name = %qn,
                        error = %e,
                        "Failed to get Redis connection for event stream"
                    );
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    return Some((
                        Err(StorageError::Unavailable(format!(
                            "Redis connection error: {}",
                            e
                        ))),
                        (pool.clone(), key.clone(), qn.clone(), last_id),
                    ));
                }
            };

            let result: Result<redis::streams::StreamReadReply, redis::RedisError> = conn
                .xread_options(
                    &[key.as_str()],
                    &[last_id.as_str()],
                    &redis::streams::StreamReadOptions::default()
                        .count(50)
                        .block(1000),
                )
                .await;

            match result {
                Ok(reply) => {
                    for stream_key_reply in reply.keys {
                        for stream_id in stream_key_reply.ids {
                            last_id = stream_id.id.clone();

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
                                                            error = %e,
                                                            "Failed to parse event from stream"
                                                        );
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    queue_name = %qn,
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
                    Some((
                        Err(StorageError::Internal("No events".to_string())),
                        (pool.clone(), key.clone(), qn.clone(), last_id),
                    ))
                }
                Err(e) if e.kind() == redis::ErrorKind::IoError => {
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    Some((
                        Err(StorageError::Unavailable("Redis IO error".to_string())),
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
                        Err(StorageError::Unavailable(format!(
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
            Err(_) => None,
        }
    });

    Box::pin(stream)
}
