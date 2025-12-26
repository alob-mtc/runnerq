//! Core traits for RunnerQ queue backends.
//!
//! This module defines the abstractions that allow RunnerQ to work with different
//! storage backends (Redis, Valkey, Kafka, SQL, etc.).
//!
//! # Architecture
//!
//! The backend abstraction is split into two main traits:
//!
//! - [`QueueBackend`]: Core queue operations (enqueue, dequeue, ack, etc.)
//! - [`InspectionBackend`]: Observability operations (stats, listing, history)
//!
//! Implementations that provide both can implement the [`Backend`] super-trait.
//!
//! # Example: Implementing a Custom Backend
//!
//! ```rust,ignore
//! use runner_q::backend::{QueueBackend, InspectionBackend, BackendError};
//! use async_trait::async_trait;
//!
//! pub struct MyBackend { /* ... */ }
//!
//! #[async_trait]
//! impl QueueBackend for MyBackend {
//!     // Implement queue operations...
//! }
//!
//! #[async_trait]
//! impl InspectionBackend for MyBackend {
//!     // Implement inspection operations...
//! }
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

use super::error::BackendError;
use crate::ActivityPriority;

// Re-export observability types used in traits
pub use crate::observability::{
    ActivityEvent, ActivityEventType, ActivitySnapshot, DeadLetterRecord,
};

// ============================================================================
// Domain Types - Backend-agnostic representations
// ============================================================================

/// An activity ready to be enqueued.
///
/// This is the input type for [`QueueBackend::enqueue`]. It contains all the
/// information needed to persist and later execute an activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedActivity {
    /// Unique identifier for this activity
    pub id: Uuid,
    /// Activity type name (used to match with handlers)
    pub activity_type: String,
    /// JSON payload to pass to the handler
    pub payload: Value,
    /// Priority level for execution ordering
    pub priority: ActivityPriority,
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Current retry count
    pub retry_count: u32,
    /// Maximum execution time in seconds
    pub timeout_seconds: u64,
    /// Base delay between retries in seconds
    pub retry_delay_seconds: u64,
    /// When to execute (None = immediately)
    pub scheduled_at: Option<DateTime<Utc>>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
    /// Idempotency key and behavior
    pub idempotency_key: Option<(String, IdempotencyBehavior)>,
    /// When the activity was created
    pub created_at: DateTime<Utc>,
}

/// An activity that has been claimed by a worker for processing.
#[derive(Debug, Clone)]
pub struct DequeuedActivity {
    /// The activity data
    pub activity: QueuedActivity,
    /// Unique lease identifier for this claim
    pub lease_id: String,
    /// Current attempt number (1-based)
    pub attempt: u32,
    /// When the lease expires
    pub lease_deadline: DateTime<Utc>,
}

/// Behavior when an activity with the same idempotency key exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdempotencyBehavior {
    /// Always create new activity, updating idempotency record
    AllowReuse,
    /// Return existing activity if key exists
    ReturnExisting,
    /// Only allow new activity if previous one failed
    AllowReuseOnFailure,
    /// Return error if key exists
    NoReuse,
}

/// Result data from a completed activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityResult {
    /// Result data (if any)
    pub data: Option<Value>,
    /// Whether the activity succeeded or failed
    pub state: ResultState,
}

/// State of an activity result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResultState {
    /// Activity completed successfully
    Ok,
    /// Activity failed
    Err,
}

/// Describes how an activity failed.
#[derive(Debug, Clone)]
pub enum FailureKind {
    /// Failure that can be retried
    Retryable { reason: String },
    /// Failure that should not be retried
    NonRetryable { reason: String },
    /// Activity execution timed out
    Timeout,
}

/// Statistics about queue state.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueueStats {
    /// Activities waiting to be processed
    pub pending: u64,
    /// Activities currently being processed
    pub processing: u64,
    /// Activities scheduled for future execution
    pub scheduled: u64,
    /// Activities in the dead letter queue
    pub dead_letter: u64,
    /// Breakdown by priority
    pub by_priority: PriorityBreakdown,
    /// Maximum configured workers (if known)
    pub max_workers: Option<usize>,
}

/// Activity counts by priority level.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PriorityBreakdown {
    pub critical: u64,
    pub high: u64,
    pub normal: u64,
    pub low: u64,
}

// ============================================================================
// QueueBackend Trait - Core queue operations
// ============================================================================

/// Core trait for queue operations.
///
/// This trait defines the essential operations that any queue backend must implement
/// to work with RunnerQ's worker engine.
///
/// # Implementation Notes
///
/// - All methods are async and should be implemented efficiently
/// - Implementations should handle their own connection pooling
/// - Errors should be mapped to [`BackendError`] variants
/// - The `lease_id` from `dequeue` must be passed to ack methods
#[async_trait]
pub trait QueueBackend: Send + Sync {
    /// Enqueue an activity for immediate or scheduled execution.
    ///
    /// If `scheduled_at` is `Some`, the activity should be stored for later
    /// execution at that time. Otherwise, it should be immediately available
    /// for dequeue.
    async fn enqueue(&self, activity: QueuedActivity) -> Result<(), BackendError>;

    /// Try to dequeue one activity for processing.
    ///
    /// Returns `None` if no activities are available within the timeout.
    /// The returned [`DequeuedActivity`] includes a `lease_id` that must be
    /// passed to `ack_success` or `ack_failure`.
    async fn dequeue(
        &self,
        worker_id: &str,
        timeout: std::time::Duration,
    ) -> Result<Option<DequeuedActivity>, BackendError>;

    /// Mark a dequeued activity as successfully completed.
    ///
    /// The `lease_id` must match the one returned from `dequeue`.
    async fn ack_success(
        &self,
        activity_id: Uuid,
        lease_id: &str,
        result: Option<Value>,
        worker_id: &str,
    ) -> Result<(), BackendError>;

    /// Mark a dequeued activity as failed.
    ///
    /// The backend should handle retry logic based on the [`FailureKind`]:
    /// - `Retryable`: Schedule for retry if attempts remain, else move to DLQ
    /// - `NonRetryable`: Mark as failed immediately
    /// - `Timeout`: Treat as retryable
    ///
    /// Returns `true` if the activity was moved to the dead letter queue.
    async fn ack_failure(
        &self,
        activity_id: Uuid,
        lease_id: &str,
        failure: FailureKind,
        worker_id: &str,
    ) -> Result<bool, BackendError>;

    /// Process scheduled activities that are ready to run.
    ///
    /// Should move activities whose `scheduled_at` time has passed to the
    /// ready queue. Returns the number of activities processed.
    async fn process_scheduled(&self) -> Result<u64, BackendError>;

    /// Requeue expired leases back to the ready queue.
    ///
    /// This is the "reaper" function that handles activities whose workers
    /// crashed or timed out. Returns the number of activities requeued.
    async fn requeue_expired(&self, batch_size: usize) -> Result<u64, BackendError>;

    /// Extend the lease for an activity currently being processed.
    ///
    /// Returns `true` if the lease was extended, `false` if not found.
    async fn extend_lease(
        &self,
        activity_id: Uuid,
        extend_by: std::time::Duration,
    ) -> Result<bool, BackendError>;

    /// Store the result of a completed activity.
    async fn store_result(
        &self,
        activity_id: Uuid,
        result: ActivityResult,
    ) -> Result<(), BackendError>;

    /// Retrieve a stored activity result.
    async fn get_result(&self, activity_id: Uuid) -> Result<Option<ActivityResult>, BackendError>;

    /// Evaluate idempotency rules before enqueueing.
    ///
    /// Returns:
    /// - `Ok(None)`: Proceed with enqueue
    /// - `Ok(Some(id))`: Return existing activity (for `ReturnExisting` behavior)
    /// - `Err(...)`: Conflict or error
    async fn check_idempotency(
        &self,
        activity: &QueuedActivity,
    ) -> Result<Option<Uuid>, BackendError>;
}

// ============================================================================
// InspectionBackend Trait - Observability operations
// ============================================================================

/// Trait for observability and inspection operations.
///
/// This trait provides read-only access to queue state for monitoring,
/// debugging, and building UIs.
#[async_trait]
pub trait InspectionBackend: Send + Sync {
    /// Get current queue statistics.
    async fn stats(&self) -> Result<QueueStats, BackendError>;

    /// List activities in the pending queue.
    async fn list_pending(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, BackendError>;

    /// List activities currently being processed.
    async fn list_processing(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, BackendError>;

    /// List activities scheduled for future execution.
    async fn list_scheduled(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, BackendError>;

    /// List completed activities (recent).
    async fn list_completed(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, BackendError>;

    /// List activities in the dead letter queue.
    async fn list_dead_letter(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, BackendError>;

    /// Get a specific activity by ID.
    async fn get_activity(
        &self,
        activity_id: Uuid,
    ) -> Result<Option<ActivitySnapshot>, BackendError>;

    /// Get recent events for an activity.
    async fn get_activity_events(
        &self,
        activity_id: Uuid,
        limit: usize,
    ) -> Result<Vec<ActivityEvent>, BackendError>;

    /// Stream real-time activity events.
    ///
    /// Returns a stream that yields events as they occur.
    fn event_stream(&self) -> BoxStream<'static, Result<ActivityEvent, BackendError>>;
}

// ============================================================================
// Backend Super-trait
// ============================================================================

/// Combined trait for backends that provide both queue and inspection capabilities.
///
/// Most backends will implement this trait, which simply requires implementing
/// both [`QueueBackend`] and [`InspectionBackend`].
pub trait Backend: QueueBackend + InspectionBackend {}

// Blanket implementation: any type implementing both traits automatically implements Backend
impl<T: QueueBackend + InspectionBackend> Backend for T {}
