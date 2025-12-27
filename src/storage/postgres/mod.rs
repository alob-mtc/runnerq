//! PostgreSQL Backend for RunnerQ
//!
//! ⚠️ **IN DEVELOPMENT - NOT READY FOR PRODUCTION USE** ⚠️
//!
//! This module provides a PostgreSQL-based storage backend implementation.
//! It is currently under active development and the API may change.
//!
//! # Status
//!
//! - [x] Basic queue operations (enqueue, dequeue, ack)
//! - [x] Multi-node concurrency with `FOR UPDATE SKIP LOCKED`
//! - [x] Idempotency support with separate tracking table
//! - [x] Event history and observability
//! - [x] Cross-process events via PostgreSQL LISTEN/NOTIFY
//! - [ ] Production testing
//! - [ ] Performance optimization
//! - [ ] Connection pool tuning
//!
//! # Features
//!
//! - **Permanent persistence** - Records never expire (unlike Redis TTL)
//! - **Multi-node safe** - Uses `FOR UPDATE SKIP LOCKED` for atomic dequeue
//! - **Full observability** - Complete event history retained
//! - **ACID transactions** - Guaranteed consistency
//! - **Cross-process events** - Uses PostgreSQL LISTEN/NOTIFY
//!
//! # Usage
//!
//! Enable the `postgres` feature in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! runner_q = { version = "0.5", features = ["postgres"] }
//! ```
//!
//! Then use the backend:
//!
//! ```rust,ignore
//! use runner_q::storage::postgres::PostgresBackend;
//! use runner_q::WorkerEngine;
//! use std::sync::Arc;
//!
//! let backend = PostgresBackend::new(
//!     "postgres://user:pass@localhost:5432/runnerq",
//!     "my_queue"
//! ).await?;
//!
//! let engine = WorkerEngine::builder()
//!     .backend(Arc::new(backend))
//!     .max_workers(4)
//!     .build()
//!     .await?;
//! ```

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde_json::{json, Value};
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use sqlx::FromRow;
use tracing::debug;
use uuid::Uuid;

use crate::activity::activity::{ActivityPriority, ActivityStatus};
use crate::observability::{ActivityEvent, ActivityEventType, ActivitySnapshot, DeadLetterRecord};
use crate::storage::{
    ActivityResult, FailureKind, IdempotencyBehavior, InspectionStorage, QueueStats, QueueStorage,
    QueuedActivity, ResultState, StorageError,
};

// ============================================================================
// PostgreSQL Backend Implementation
// ============================================================================

/// PostgreSQL-based storage backend for RunnerQ.
///
/// ⚠️ **IN DEVELOPMENT** - This backend is not yet ready for production use.
///
/// This backend provides:
/// - **Permanent persistence** - No TTL, records are kept forever
/// - **Multi-node concurrency** - Safe dequeue with `FOR UPDATE SKIP LOCKED`
/// - **Full ACID guarantees** - Transactional consistency
/// - **Cross-process events** - Uses PostgreSQL LISTEN/NOTIFY for real-time events
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    queue_name: String,
    default_lease_ms: i64,
}

impl PostgresBackend {
    /// Validate that a queue name is safe for use in PostgreSQL identifiers.
    ///
    /// Valid queue names must:
    /// - Be non-empty
    /// - Start with a letter (a-z, A-Z) or underscore
    /// - Contain only letters, digits (0-9), and underscores
    /// - Be at most 63 characters (PostgreSQL identifier limit)
    fn validate_queue_name(queue_name: &str) -> Result<(), StorageError> {
        if queue_name.is_empty() {
            return Err(StorageError::Configuration(
                "queue_name cannot be empty".to_string(),
            ));
        }

        // PostgreSQL identifier max length is 63 bytes (NAMEDATALEN - 1)
        // We reserve some space for the "runnerq_events_" prefix (15 chars)
        if queue_name.len() > 48 {
            return Err(StorageError::Configuration(format!(
                "queue_name '{}' is too long (max 48 characters)",
                queue_name
            )));
        }

        let mut chars = queue_name.chars();

        // First character must be a letter or underscore
        if let Some(first) = chars.next() {
            if !first.is_ascii_alphabetic() && first != '_' {
                return Err(StorageError::Configuration(format!(
                    "queue_name '{}' must start with a letter or underscore",
                    queue_name
                )));
            }
        }

        // Remaining characters must be letters, digits, or underscores
        for c in chars {
            if !c.is_ascii_alphanumeric() && c != '_' {
                return Err(StorageError::Configuration(format!(
                    "queue_name '{}' contains invalid character '{}'; only letters, digits, and underscores are allowed",
                    queue_name, c
                )));
            }
        }

        Ok(())
    }

    /// Create a new PostgreSQL backend.
    ///
    /// # Arguments
    ///
    /// * `database_url` - PostgreSQL connection URL (e.g., `postgres://user:pass@host:5432/db`)
    /// * `queue_name` - Name of the queue (used for namespacing). Must be a valid identifier:
    ///   starts with letter or underscore, contains only letters, digits, underscores.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let backend = PostgresBackend::new(
    ///     "postgres://postgres:password@localhost:5432/runnerq",
    ///     "my_queue"
    /// ).await?;
    /// ```
    pub async fn new(database_url: &str, queue_name: &str) -> Result<Self, StorageError> {
        Self::with_config(database_url, queue_name, 30_000, 10).await
    }

    /// Create a new PostgreSQL backend with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `database_url` - PostgreSQL connection URL
    /// * `queue_name` - Name of the queue. Must be a valid identifier:
    ///   starts with letter or underscore, contains only letters, digits, underscores.
    /// * `default_lease_ms` - Default lease duration in milliseconds
    /// * `pool_size` - Maximum number of connections in the pool
    pub async fn with_config(
        database_url: &str,
        queue_name: &str,
        default_lease_ms: i64,
        pool_size: u32,
    ) -> Result<Self, StorageError> {
        // Validate queue_name before using it
        Self::validate_queue_name(queue_name)?;

        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .connect(database_url)
            .await
            .map_err(|e| {
                StorageError::Unavailable(format!("Failed to connect to PostgreSQL: {}", e))
            })?;

        let backend = Self {
            pool,
            queue_name: queue_name.to_string(),
            default_lease_ms,
        };

        // Initialize schema
        backend.init_schema().await?;

        Ok(backend)
    }

    /// Initialize the database schema.
    async fn init_schema(&self) -> Result<(), StorageError> {
        // Use raw_sql to execute multiple statements (CREATE TABLE, CREATE INDEX, etc.)
        sqlx::raw_sql(SCHEMA_SQL)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to initialize schema: {}", e)))?;
        Ok(())
    }

    /// Record an activity event.
    ///
    /// This method:
    /// 1. Stores the event permanently in the database
    /// 2. Uses PostgreSQL NOTIFY to broadcast to all listeners (cross-process)
    async fn record_event(
        &self,
        activity_id: Uuid,
        event_type: ActivityEventType,
        worker_id: Option<&str>,
        detail: Option<Value>,
    ) -> Result<(), StorageError> {
        let event = ActivityEvent {
            activity_id,
            timestamp: Utc::now(),
            event_type: event_type.clone(),
            worker_id: worker_id.map(String::from),
            detail: detail.clone(),
        };

        // Store in database (permanent record)
        sqlx::query(
            r#"
            INSERT INTO runnerq_events (activity_id, queue_name, event_type, worker_id, detail, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(activity_id)
        .bind(&self.queue_name)
        .bind(serde_json::to_string(&event_type).unwrap_or_default())
        .bind(worker_id)
        .bind(detail)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to record event: {}", e)))?;

        // Broadcast via PostgreSQL NOTIFY (cross-process!)
        // This allows UI running in a separate process to receive real-time events
        let channel = self.notification_channel();
        let payload = serde_json::to_string(&event)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Escape single quotes in payload for SQL
        let escaped_payload = payload.replace('\'', "''");

        // Quote the channel name as a PostgreSQL identifier (double-quote and escape any internal double-quotes)
        let quoted_channel = Self::quote_identifier(&channel);

        sqlx::query(&format!("NOTIFY {}, '{}'", quoted_channel, escaped_payload))
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to notify: {}", e)))?;

        Ok(())
    }

    /// Quote a string as a PostgreSQL identifier (double-quoted).
    ///
    /// This escapes any internal double-quotes by doubling them.
    fn quote_identifier(name: &str) -> String {
        // Escape internal double-quotes by doubling them
        let escaped = name.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    }

    /// Get the PostgreSQL NOTIFY channel name for this queue.
    ///
    /// Since queue_name is validated at construction to only contain
    /// [A-Za-z0-9_], this produces a safe identifier.
    fn notification_channel(&self) -> String {
        // Channel names must be valid identifiers
        // queue_name is already validated at construction to only contain safe characters
        format!("runnerq_events_{}", self.queue_name)
    }

    /// Convert priority enum to integer for sorting.
    fn priority_to_int(priority: &ActivityPriority) -> i32 {
        match priority {
            ActivityPriority::Critical => 3,
            ActivityPriority::High => 2,
            ActivityPriority::Normal => 1,
            ActivityPriority::Low => 0,
        }
    }

    /// Convert integer back to priority enum.
    fn int_to_priority(val: i32) -> ActivityPriority {
        match val {
            3 => ActivityPriority::Critical,
            2 => ActivityPriority::High,
            1 => ActivityPriority::Normal,
            _ => ActivityPriority::Low,
        }
    }
}

// ============================================================================
// QueueStorage Implementation
// ============================================================================

#[async_trait]
impl QueueStorage for PostgresBackend {
    async fn enqueue(&self, activity: QueuedActivity) -> Result<(), StorageError> {
        let status = if activity.scheduled_at.is_some() {
            "scheduled"
        } else {
            "pending"
        };

        let metadata_json = serde_json::to_value(&activity.metadata)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        // Extract idempotency key for storage (display in UI)
        let idempotency_key = activity.idempotency_key.as_ref().map(|(k, _)| k.clone());

        // Simple insert - idempotency is handled by check_idempotency which should be called first
        sqlx::query(
            r#"
            INSERT INTO runnerq_activities (
                id, queue_name, activity_type, payload, priority, status,
                created_at, scheduled_at, retry_count, max_retries,
                timeout_seconds, retry_delay_seconds, metadata, idempotency_key
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
        )
        .bind(activity.id)
        .bind(&self.queue_name)
        .bind(&activity.activity_type)
        .bind(&activity.payload)
        .bind(Self::priority_to_int(&activity.priority))
        .bind(status)
        .bind(activity.created_at)
        .bind(activity.scheduled_at)
        .bind(activity.retry_count as i32)
        .bind(activity.max_retries as i32)
        .bind(activity.timeout_seconds as i64)
        .bind(activity.retry_delay_seconds as i64)
        .bind(metadata_json)
        .bind(idempotency_key)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to enqueue activity: {}", e)))?;

        let (event_type, detail) = if activity.scheduled_at.is_some() {
            (
                ActivityEventType::Scheduled,
                json!({ "scheduled_at": activity.scheduled_at }),
            )
        } else {
            (
                ActivityEventType::Enqueued,
                json!({
                    "priority": format!("{:?}", activity.priority),
                    "scheduled_at": activity.scheduled_at
                }),
            )
        };

        self.record_event(activity.id, event_type, None, Some(detail))
            .await?;

        Ok(())
    }

    async fn dequeue(
        &self,
        worker_id: &str,
        _timeout: Duration,
    ) -> Result<Option<QueuedActivity>, StorageError> {
        let deadline = Utc::now() + chrono::Duration::milliseconds(self.default_lease_ms);

        // Use FOR UPDATE SKIP LOCKED for safe multi-node dequeue
        // This atomically claims a job without blocking other workers
        let result: Option<ActivityRow> = sqlx::query_as(
            r#"
            UPDATE runnerq_activities
            SET status = 'processing',
                current_worker_id = $1,
                lease_deadline_ms = $2,
                started_at = $3
            WHERE id = (
                SELECT id FROM runnerq_activities
                WHERE queue_name = $4
                  AND status = 'pending'
                  AND (scheduled_at IS NULL OR scheduled_at <= $5)
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            "#,
        )
        .bind(worker_id)
        .bind(deadline.timestamp_millis())
        .bind(Utc::now())
        .bind(&self.queue_name)
        .bind(Utc::now())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to dequeue: {}", e)))?;

        match result {
            Some(row) => {
                let activity = row.to_queued_activity()?;

                self.record_event(
                    activity.id,
                    ActivityEventType::Dequeued,
                    Some(worker_id),
                    Some(json!({ "lease_deadline_ms": deadline.timestamp_millis() })),
                )
                .await?;

                // Extend lease if default lease is shorter than activity timeout
                let default_lease_secs = (self.default_lease_ms / 1000) as u64;
                if default_lease_secs < activity.timeout_seconds {
                    let additional_secs = activity.timeout_seconds - default_lease_secs;
                    let lease_buffer = 10;
                    let _ = self
                        .extend_lease(
                            activity.id,
                            Duration::from_secs(additional_secs + lease_buffer),
                        )
                        .await;
                }

                debug!(
                    activity_id = %activity.id,
                    activity_type = ?activity.activity_type,
                    priority = ?activity.priority,
                    "Activity claimed"
                );

                Ok(Some(activity))
            }
            None => Ok(None),
        }
    }

    async fn ack_success(
        &self,
        activity_id: Uuid,
        result: Option<Value>,
        worker_id: &str,
    ) -> Result<(), StorageError> {
        let now = Utc::now();

        // Get activity type for event detail
        let activity_type: Option<(String,)> =
            sqlx::query_as(r#"SELECT activity_type FROM runnerq_activities WHERE id = $1"#)
                .bind(activity_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    StorageError::Internal(format!("Failed to get activity type: {}", e))
                })?;

        // Update activity status
        sqlx::query(
            r#"
            UPDATE runnerq_activities
            SET status = 'completed',
                completed_at = $1,
                last_worker_id = $2,
                current_worker_id = NULL,
                lease_deadline_ms = NULL
            WHERE id = $3 AND queue_name = $4
            "#,
        )
        .bind(now)
        .bind(worker_id)
        .bind(activity_id)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to ack success: {}", e)))?;

        // Store result if provided
        if let Some(data) = result.clone() {
            self.store_result(
                activity_id,
                ActivityResult {
                    data: Some(data.clone()),
                    state: ResultState::Ok,
                },
            )
            .await?;
        }

        self.record_event(
            activity_id,
            ActivityEventType::Completed,
            Some(worker_id),
            Some(json!({
                "activity_type": activity_type.map(|t| t.0).unwrap_or_default()
            })),
        )
        .await?;

        // Record ResultStored event if we stored a result
        if let Some(data) = result {
            self.record_event(
                activity_id,
                ActivityEventType::ResultStored,
                None,
                Some(json!({
                    "state": "Ok",
                    "data": data
                })),
            )
            .await?;
        }

        Ok(())
    }

    async fn ack_failure(
        &self,
        activity_id: Uuid,
        failure: FailureKind,
        worker_id: &str,
    ) -> Result<bool, StorageError> {
        let now = Utc::now();

        let (error_message, retryable) = match &failure {
            FailureKind::Retryable { reason } => (reason.clone(), true),
            FailureKind::NonRetryable { reason } => (reason.clone(), false),
            FailureKind::Timeout => ("Activity execution timed out".to_string(), true),
        };

        // Get current activity state
        let activity: Option<ActivityRow> =
            sqlx::query_as(r#"SELECT * FROM runnerq_activities WHERE id = $1 AND queue_name = $2"#)
                .bind(activity_id)
                .bind(&self.queue_name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Failed to get activity: {}", e)))?;

        let activity = match activity {
            Some(a) => a,
            None => {
                return Err(StorageError::NotFound(format!(
                    "Activity {} not found",
                    activity_id
                )))
            }
        };

        // Non-retryable failures go straight to Failed status (not DLQ)
        if !retryable {
            sqlx::query(
                r#"
                UPDATE runnerq_activities
                SET status = 'failed',
                    completed_at = $1,
                    last_error = $2,
                    last_error_at = $3,
                    last_worker_id = $4,
                    current_worker_id = NULL,
                    lease_deadline_ms = NULL
                WHERE id = $5
                "#,
            )
            .bind(now)
            .bind(&error_message)
            .bind(now)
            .bind(worker_id)
            .bind(activity_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to mark as failed: {}", e)))?;

            // Store failure result
            self.store_result(
                activity_id,
                ActivityResult {
                    data: Some(json!({ "error": error_message })),
                    state: ResultState::Err,
                },
            )
            .await?;

            self.record_event(
                activity_id,
                ActivityEventType::Failed,
                Some(worker_id),
                Some(json!({ "retryable": false, "error": error_message })),
            )
            .await?;

            return Ok(false);
        }

        // Check if we can retry: max_retries == 0 means infinite retries
        // Use retry_count + 1 < max_retries to match Redis behavior
        let can_retry = activity.max_retries == 0
            || (activity.retry_count as u32 + 1) < (activity.max_retries as u32);

        if can_retry {
            // Calculate retry delay with exponential backoff
            let base_delay = activity.retry_delay_seconds;
            let backoff_multiplier = 2_i64.pow(activity.retry_count as u32 + 1);
            let retry_delay = base_delay * backoff_multiplier;
            let scheduled_at = now + chrono::Duration::seconds(retry_delay);

            // Update for retry - use 'retrying' status to match Redis
            sqlx::query(
                r#"
                UPDATE runnerq_activities
                SET status = 'retrying',
                    retry_count = retry_count + 1,
                    scheduled_at = $1,
                    last_error = $2,
                    last_error_at = $3,
                    last_worker_id = $4,
                    current_worker_id = NULL,
                    lease_deadline_ms = NULL
                WHERE id = $5
                "#,
            )
            .bind(scheduled_at)
            .bind(&error_message)
            .bind(now)
            .bind(worker_id)
            .bind(activity_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to schedule retry: {}", e)))?;

            self.record_event(
                activity_id,
                ActivityEventType::Retrying,
                Some(worker_id),
                Some(json!({
                    "retry_count": activity.retry_count + 1,
                    "scheduled_at": scheduled_at,
                    "error": error_message
                })),
            )
            .await?;

            return Ok(false);
        }

        // Move to dead letter queue - exhausted all retries
        sqlx::query(
            r#"
            UPDATE runnerq_activities
            SET status = 'dead_letter',
                completed_at = $1,
                last_error = $2,
                last_error_at = $3,
                last_worker_id = $4,
                current_worker_id = NULL,
                lease_deadline_ms = NULL
            WHERE id = $5
            "#,
        )
        .bind(now)
        .bind(&error_message)
        .bind(now)
        .bind(worker_id)
        .bind(activity_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to move to DLQ: {}", e)))?;

        // Store failure result
        self.store_result(
            activity_id,
            ActivityResult {
                data: Some(json!({ "error": error_message })),
                state: ResultState::Err,
            },
        )
        .await?;

        self.record_event(
            activity_id,
            ActivityEventType::DeadLetter,
            Some(worker_id),
            Some(json!({ "error": error_message })),
        )
        .await?;

        Ok(true)
    }

    async fn process_scheduled(&self) -> Result<u64, StorageError> {
        let now = Utc::now();

        let result = sqlx::query(
            r#"
            UPDATE runnerq_activities
            SET status = 'pending', scheduled_at = NULL
            WHERE queue_name = $1
              AND status IN ('scheduled', 'retrying')
              AND scheduled_at <= $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to process scheduled: {}", e)))?;

        Ok(result.rows_affected())
    }

    async fn requeue_expired(&self, batch_size: usize) -> Result<u64, StorageError> {
        let now = Utc::now();
        let now_ms = now.timestamp_millis();

        let result = sqlx::query(
            r#"
            UPDATE runnerq_activities
            SET status = 'pending',
                current_worker_id = NULL,
                lease_deadline_ms = NULL
            WHERE id IN (
                SELECT id FROM runnerq_activities
                WHERE queue_name = $1
                  AND status = 'processing'
                  AND lease_deadline_ms < $2
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            "#,
        )
        .bind(&self.queue_name)
        .bind(now_ms)
        .bind(batch_size as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to requeue expired: {}", e)))?;

        Ok(result.rows_affected())
    }

    async fn extend_lease(
        &self,
        activity_id: Uuid,
        extend_by: Duration,
    ) -> Result<bool, StorageError> {
        let new_deadline = Utc::now() + chrono::Duration::from_std(extend_by).unwrap_or_default();

        let result = sqlx::query(
            r#"
            UPDATE runnerq_activities
            SET lease_deadline_ms = $1
            WHERE id = $2 AND queue_name = $3 AND status = 'processing'
            "#,
        )
        .bind(new_deadline.timestamp_millis())
        .bind(activity_id)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to extend lease: {}", e)))?;

        if result.rows_affected() > 0 {
            self.record_event(
                activity_id,
                ActivityEventType::LeaseExtended,
                None,
                Some(json!({
                    "new_deadline_ms": new_deadline.timestamp_millis(),
                    "extend_by_ms": extend_by.as_millis() as i64
                })),
            )
            .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn store_result(
        &self,
        activity_id: Uuid,
        result: ActivityResult,
    ) -> Result<(), StorageError> {
        let state_str = match result.state {
            ResultState::Ok => "Ok",
            ResultState::Err => "Err",
        };

        sqlx::query(
            r#"
            INSERT INTO runnerq_results (activity_id, queue_name, state, data, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (activity_id) DO UPDATE
            SET state = $3, data = $4, created_at = $5
            "#,
        )
        .bind(activity_id)
        .bind(&self.queue_name)
        .bind(state_str)
        .bind(&result.data)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    async fn get_result(&self, activity_id: Uuid) -> Result<Option<ActivityResult>, StorageError> {
        let row: Option<(String, Option<Value>)> =
            sqlx::query_as(r#"SELECT state, data FROM runnerq_results WHERE activity_id = $1"#)
                .bind(activity_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Failed to get result: {}", e)))?;

        Ok(row.map(|(state, data)| ActivityResult {
            data,
            state: if state == "Ok" {
                ResultState::Ok
            } else {
                ResultState::Err
            },
        }))
    }

    async fn check_idempotency(
        &self,
        activity: &QueuedActivity,
    ) -> Result<Option<Uuid>, StorageError> {
        let (key, behavior) = match &activity.idempotency_key {
            Some((k, b)) => (k, b),
            None => return Ok(None),
        };

        // Try to atomically claim the idempotency key (like Redis SET NX)
        // Uses INSERT ... ON CONFLICT DO NOTHING and checks rows_affected
        let claim_result = sqlx::query(
            r#"
            INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
            VALUES ($1, $2, $3, NOW(), NOW())
            ON CONFLICT (queue_name, idempotency_key) DO NOTHING
            "#,
        )
        .bind(&self.queue_name)
        .bind(key)
        .bind(activity.id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to claim idempotency key: {}", e)))?;

        if claim_result.rows_affected() > 0 {
            // Successfully claimed the key - proceed with enqueue
            return Ok(None);
        }

        // Key already exists - get the existing activity ID and its status
        let existing: Option<(Uuid, Option<String>)> = sqlx::query_as(
            r#"
            SELECT i.activity_id, a.status
            FROM runnerq_idempotency i
            LEFT JOIN runnerq_activities a ON i.activity_id = a.id
            WHERE i.queue_name = $1 AND i.idempotency_key = $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to get existing key: {}", e)))?;

        let (existing_id, status) = match existing {
            Some((id, status)) => (id, status),
            None => {
                // Key was removed between our insert attempt and now - retry claim
                return Box::pin(self.check_idempotency(activity)).await;
            }
        };

        match behavior {
            IdempotencyBehavior::ReturnExisting => Ok(Some(existing_id)),

            IdempotencyBehavior::AllowReuse => {
                // Update pointer to new activity (old activity remains in history)
                sqlx::query(
                    r#"
                    UPDATE runnerq_idempotency
                    SET activity_id = $1, updated_at = NOW()
                    WHERE queue_name = $2 AND idempotency_key = $3
                    "#,
                )
                .bind(activity.id)
                .bind(&self.queue_name)
                .bind(key)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Failed to update key: {}", e)))?;
                Ok(None)
            }

            IdempotencyBehavior::AllowReuseOnFailure => {
                // Only allow reuse if existing activity failed
                match status.as_deref() {
                    Some("dead_letter") | Some("failed") => {
                        // Atomically update pointer only if status is still failed
                        // Old activity remains in history - we just point to new one
                        let update_result = sqlx::query(
                            r#"
                            UPDATE runnerq_idempotency i
                            SET activity_id = $1, updated_at = NOW()
                            FROM runnerq_activities a
                            WHERE i.queue_name = $2
                              AND i.idempotency_key = $3
                              AND i.activity_id = a.id
                              AND a.status IN ('dead_letter', 'failed')
                            "#,
                        )
                        .bind(activity.id)
                        .bind(&self.queue_name)
                        .bind(key)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| {
                            StorageError::Internal(format!("Failed to update key: {}", e))
                        })?;

                        if update_result.rows_affected() > 0 {
                            Ok(None)
                        } else {
                            Err(StorageError::IdempotencyConflict(format!(
                                "Idempotency key '{}' was claimed by another request",
                                key
                            )))
                        }
                    }
                    _ => Err(StorageError::IdempotencyConflict(format!(
                        "Idempotency key '{}' exists with status '{}'",
                        key,
                        status.unwrap_or_default()
                    ))),
                }
            }

            IdempotencyBehavior::NoReuse => Err(StorageError::DuplicateActivity(format!(
                "Activity with idempotency key '{}' already exists: {}",
                key, existing_id
            ))),
        }
    }
}

// ============================================================================
// InspectionStorage Implementation
// ============================================================================

#[async_trait]
impl InspectionStorage for PostgresBackend {
    async fn stats(&self) -> Result<QueueStats, StorageError> {
        let counts: Vec<(String, i64)> = sqlx::query_as(
            r#"
            SELECT status, COUNT(*) as count
            FROM runnerq_activities
            WHERE queue_name = $1
            GROUP BY status
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to get stats: {}", e)))?;

        let priority_counts: Vec<(i32, i64)> = sqlx::query_as(
            r#"
            SELECT priority, COUNT(*) as count
            FROM runnerq_activities
            WHERE queue_name = $1 AND status = 'pending'
            GROUP BY priority
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to get priority stats: {}", e)))?;

        let mut stats = QueueStats::default();

        for (status, count) in counts {
            match status.as_str() {
                "pending" => stats.pending = count as u64,
                "processing" => stats.processing = count as u64,
                "scheduled" | "retrying" => stats.scheduled += count as u64,
                "dead_letter" => stats.dead_letter = count as u64,
                _ => {}
            }
        }

        for (priority, count) in priority_counts {
            match priority {
                3 => stats.by_priority.critical = count as u64,
                2 => stats.by_priority.high = count as u64,
                1 => stats.by_priority.normal = count as u64,
                0 => stats.by_priority.low = count as u64,
                _ => {}
            }
        }

        Ok(stats)
    }

    async fn list_pending(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        self.list_by_status("pending", offset, limit).await
    }

    async fn list_processing(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        self.list_by_status("processing", offset, limit).await
    }

    async fn list_scheduled(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        // Include both 'scheduled' and 'retrying' statuses
        let rows: Vec<ActivityRow> = sqlx::query_as(
            r#"
            SELECT * FROM runnerq_activities
            WHERE queue_name = $1 AND status IN ('scheduled', 'retrying')
            ORDER BY scheduled_at ASC, created_at ASC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to list scheduled: {}", e)))?;

        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(row.to_snapshot()?);
        }

        Ok(snapshots)
    }

    async fn list_completed(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        self.list_by_status("completed", offset, limit).await
    }

    async fn list_dead_letter(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, StorageError> {
        let rows: Vec<ActivityRow> = sqlx::query_as(
            r#"
            SELECT * FROM runnerq_activities
            WHERE queue_name = $1 AND status = 'dead_letter'
            ORDER BY completed_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to list dead letter: {}", e)))?;

        let mut records = Vec::new();
        for row in rows {
            let snapshot = row.to_snapshot()?;
            records.push(DeadLetterRecord {
                activity: snapshot,
                error: row.last_error.unwrap_or_default(),
                failed_at: row.completed_at.unwrap_or_else(Utc::now),
            });
        }

        Ok(records)
    }

    async fn get_activity(
        &self,
        activity_id: Uuid,
    ) -> Result<Option<ActivitySnapshot>, StorageError> {
        let row: Option<ActivityRow> =
            sqlx::query_as(r#"SELECT * FROM runnerq_activities WHERE id = $1 AND queue_name = $2"#)
                .bind(activity_id)
                .bind(&self.queue_name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Failed to get activity: {}", e)))?;

        match row {
            Some(r) => Ok(Some(r.to_snapshot()?)),
            None => Ok(None),
        }
    }

    async fn get_activity_events(
        &self,
        activity_id: Uuid,
        limit: usize,
    ) -> Result<Vec<ActivityEvent>, StorageError> {
        let rows: Vec<EventRow> = sqlx::query_as(
            r#"
            SELECT * FROM runnerq_events
            WHERE activity_id = $1
            ORDER BY created_at ASC
            LIMIT $2
            "#,
        )
        .bind(activity_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to get events: {}", e)))?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row.to_event()?);
        }

        Ok(events)
    }

    fn event_stream(&self) -> BoxStream<'static, Result<ActivityEvent, StorageError>> {
        let pool = self.pool.clone();
        let channel = self.notification_channel();

        Box::pin(async_stream::stream! {
            // Create a dedicated listener connection
            let mut listener = match PgListener::connect_with(&pool).await {
                Ok(l) => l,
                Err(e) => {
                    yield Err(StorageError::Unavailable(format!("Failed to create listener: {}", e)));
                    return;
                }
            };

            // Subscribe to the notification channel
            if let Err(e) = listener.listen(&channel).await {
                yield Err(StorageError::Internal(format!("Failed to listen on channel {}: {}", channel, e)));
                return;
            }

            // Listen for notifications
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        // Parse the event from the notification payload
                        match serde_json::from_str::<ActivityEvent>(notification.payload()) {
                            Ok(event) => yield Ok(event),
                            Err(e) => {
                                // Log parse error but continue listening
                                tracing::warn!(
                                    error = %e,
                                    payload = notification.payload(),
                                    "Failed to parse event from notification"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(StorageError::Internal(format!("Listener error: {}", e)));
                        break;
                    }
                }
            }
        })
    }
}

impl PostgresBackend {
    async fn list_by_status(
        &self,
        status: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        let rows: Vec<ActivityRow> = sqlx::query_as(
            r#"
            SELECT * FROM runnerq_activities
            WHERE queue_name = $1 AND status = $2
            ORDER BY priority DESC, created_at ASC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(&self.queue_name)
        .bind(status)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Failed to list {}: {}", status, e)))?;

        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(row.to_snapshot()?);
        }

        Ok(snapshots)
    }
}

// ============================================================================
// Database Row Types
// ============================================================================

#[derive(Debug, FromRow)]
struct ActivityRow {
    id: Uuid,
    queue_name: String,
    activity_type: String,
    payload: Value,
    priority: i32,
    status: String,
    created_at: DateTime<Utc>,
    scheduled_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    lease_deadline_ms: Option<i64>,
    current_worker_id: Option<String>,
    last_worker_id: Option<String>,
    retry_count: i32,
    max_retries: i32,
    timeout_seconds: i64,
    retry_delay_seconds: i64,
    last_error: Option<String>,
    last_error_at: Option<DateTime<Utc>>,
    metadata: Option<Value>,
    idempotency_key: Option<String>,
}

impl ActivityRow {
    fn to_queued_activity(&self) -> Result<QueuedActivity, StorageError> {
        let metadata: HashMap<String, String> = self
            .metadata
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        Ok(QueuedActivity {
            id: self.id,
            activity_type: self.activity_type.clone(),
            payload: self.payload.clone(),
            priority: PostgresBackend::int_to_priority(self.priority),
            max_retries: self.max_retries as u32,
            retry_count: self.retry_count as u32,
            timeout_seconds: self.timeout_seconds as u64,
            retry_delay_seconds: self.retry_delay_seconds as u64,
            scheduled_at: self.scheduled_at,
            metadata,
            idempotency_key: self
                .idempotency_key
                .as_ref()
                .map(|k| (k.clone(), IdempotencyBehavior::ReturnExisting)),
            created_at: self.created_at,
        })
    }

    fn to_snapshot(&self) -> Result<ActivitySnapshot, StorageError> {
        let metadata: HashMap<String, String> = self
            .metadata
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let status = match self.status.as_str() {
            "pending" => ActivityStatus::Pending,
            "processing" => ActivityStatus::Running,
            "completed" => ActivityStatus::Completed,
            "scheduled" => ActivityStatus::Pending,
            "retrying" => ActivityStatus::Retrying,
            "failed" => ActivityStatus::Failed,
            "dead_letter" => ActivityStatus::DeadLetter,
            _ => ActivityStatus::Pending,
        };

        Ok(ActivitySnapshot {
            id: self.id,
            activity_type: self.activity_type.clone(),
            payload: self.payload.clone(),
            priority: PostgresBackend::int_to_priority(self.priority),
            status,
            created_at: self.created_at,
            scheduled_at: self.scheduled_at,
            started_at: self.started_at,
            completed_at: self.completed_at,
            current_worker_id: self.current_worker_id.clone(),
            last_worker_id: self.last_worker_id.clone(),
            retry_count: self.retry_count as u32,
            max_retries: self.max_retries as u32,
            timeout_seconds: self.timeout_seconds as u64,
            retry_delay_seconds: self.retry_delay_seconds as u64,
            metadata,
            last_error: self.last_error.clone(),
            last_error_at: self.last_error_at,
            status_updated_at: self
                .started_at
                .or(self.completed_at)
                .unwrap_or(self.created_at),
            score: None,
            lease_deadline_ms: self.lease_deadline_ms,
            processing_member: self.current_worker_id.clone(),
            idempotency_key: self.idempotency_key.clone(),
        })
    }
}

#[derive(Debug, FromRow)]
struct EventRow {
    #[allow(dead_code)]
    id: i64,
    activity_id: Uuid,
    #[allow(dead_code)]
    queue_name: String,
    event_type: String,
    worker_id: Option<String>,
    detail: Option<Value>,
    created_at: DateTime<Utc>,
}

impl EventRow {
    fn to_event(&self) -> Result<ActivityEvent, StorageError> {
        // event_type is stored as JSON string (e.g., "\"Enqueued\""), so deserialize directly
        let event_type: ActivityEventType =
            serde_json::from_str(&self.event_type).unwrap_or(ActivityEventType::Enqueued);

        Ok(ActivityEvent {
            activity_id: self.activity_id,
            timestamp: self.created_at,
            event_type,
            worker_id: self.worker_id.clone(),
            detail: self.detail.clone(),
        })
    }
}

// ============================================================================
// Database Schema
// ============================================================================

const SCHEMA_SQL: &str = r#"
-- Activities table (permanent - no TTL)
CREATE TABLE IF NOT EXISTS runnerq_activities (
    id UUID PRIMARY KEY,
    queue_name TEXT NOT NULL,
    activity_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    lease_deadline_ms BIGINT,
    current_worker_id TEXT,
    last_worker_id TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    timeout_seconds BIGINT NOT NULL DEFAULT 300,
    retry_delay_seconds BIGINT NOT NULL DEFAULT 60,
    last_error TEXT,
    last_error_at TIMESTAMPTZ,
    metadata JSONB,
    idempotency_key TEXT
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_runnerq_activities_queue_status
    ON runnerq_activities(queue_name, status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_runnerq_activities_scheduled
    ON runnerq_activities(queue_name, scheduled_at)
    WHERE status IN ('scheduled', 'retrying');
CREATE INDEX IF NOT EXISTS idx_runnerq_activities_processing
    ON runnerq_activities(queue_name, lease_deadline_ms)
    WHERE status = 'processing';

-- Idempotency keys table (separate storage like Redis)
-- This is a "current pointer" to which activity owns the key
-- Old activities are NEVER deleted - history is preserved
CREATE TABLE IF NOT EXISTS runnerq_idempotency (
    queue_name TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    activity_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (queue_name, idempotency_key)
);

-- Events table (permanent - full history)
CREATE TABLE IF NOT EXISTS runnerq_events (
    id BIGSERIAL PRIMARY KEY,
    activity_id UUID NOT NULL,
    queue_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    worker_id TEXT,
    detail JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_runnerq_events_activity
    ON runnerq_events(activity_id, created_at DESC);

-- Results table (permanent)
CREATE TABLE IF NOT EXISTS runnerq_results (
    activity_id UUID PRIMARY KEY,
    queue_name TEXT NOT NULL,
    state TEXT NOT NULL,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"#;
