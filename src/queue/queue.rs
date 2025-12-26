//! Activity queue trait and result types.
//!
//! This module defines the internal queue trait used by RunnerQ's worker engine.
//! The trait is implemented by `BackendQueueAdapter` to bridge the `Backend` abstraction.

use crate::activity::activity::Activity;
use crate::runner::error::WorkerError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Trait defining the interface for activity queue operations.
///
/// This is an internal trait used by the worker engine for processing activities.
/// Custom backends should implement the [`Backend`] trait instead, which is
/// automatically adapted to this trait via `BackendQueueAdapter`.
#[async_trait]
pub(crate) trait ActivityQueueTrait: Send + Sync {
    /// Enqueue an activity for processing
    async fn enqueue(&self, activity: Activity) -> Result<(), WorkerError>;

    /// Dequeue the next available activity with timeout
    async fn dequeue(
        &self,
        timeout: Duration,
        worker_id: &str,
    ) -> Result<Option<Activity>, WorkerError>;

    /// Assign a worker to an activity that has been dequeued.
    async fn assign_worker(
        &self,
        activity_id: uuid::Uuid,
        worker_id: &str,
    ) -> Result<(), WorkerError>;

    /// Mark a activity as completed
    async fn mark_completed(&self, activity: &Activity, worker_id: &str)
        -> Result<(), WorkerError>;

    /// Mark a activity as failed and optionally requeue for retry.
    ///
    /// Returns `Ok(true)` if the activity was moved to dead letter queue,
    /// `Ok(false)` if it was retried or marked as failed without dead-lettering.
    async fn mark_failed(
        &self,
        activity: Activity,
        error_message: String,
        retryable: bool,
        worker_id: &str,
    ) -> Result<bool, WorkerError>;

    /// Schedule an activity for future execution
    async fn schedule_activity(&self, activity: Activity) -> Result<(), WorkerError>;

    /// Process scheduled activities that are ready to run
    async fn process_scheduled_activities(&self) -> Result<Vec<Activity>, WorkerError>;

    /// Requeue expired processing items back to main queue (reaper)
    async fn requeue_expired(&self, max_to_process: usize) -> Result<u64, WorkerError>;

    /// Evaluate idempotency rules for an activity using atomic operations.
    ///
    /// This method is thread-safe and uses atomic Redis operations to prevent race conditions.
    /// It should be called early in the activity execution flow, before enqueueing.
    ///
    /// Returns:
    /// - `Ok(None)` if the activity should proceed with enqueue
    /// - `Ok(Some(existing_id))` if `ReturnExisting` behavior is used and an existing activity was found
    /// - `Err(...)` if there was an error or conflict
    async fn evaluate_idempotency_rule(
        &self,
        activity: &Activity,
    ) -> Result<Option<uuid::Uuid>, WorkerError>;

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

/// State of an activity result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ResultState {
    /// Activity completed successfully
    Ok,
    /// Activity failed
    Err,
}

/// Result data from a completed activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ActivityResult {
    /// Result data (if any)
    pub data: Option<serde_json::Value>,
    /// Whether the activity succeeded or failed
    pub state: ResultState,
}
