use std::sync::Arc;

use crate::observability::models::{ActivityEvent, ActivitySnapshot, DeadLetterRecord, QueueStats};
use crate::runner::error::WorkerError;
use crate::storage::InspectionStorage;
use futures::stream::{Stream, StreamExt};
use uuid::Uuid;

/// Queue inspector for observability operations.
///
/// This struct provides read-only access to queue state for monitoring and debugging.
/// It uses a backend implementing the `Storage` trait for all operations.
#[derive(Clone)]
pub struct QueueInspector {
    max_workers: Option<usize>,
    backend: Arc<dyn InspectionStorage>,
}

impl QueueInspector {
    /// Create a new inspector from a backend.
    pub fn new(backend: Arc<dyn InspectionStorage>) -> Self {
        Self {
            max_workers: None,
            backend,
        }
    }

    /// Set the maximum number of workers for stats reporting.
    pub fn with_max_workers(mut self, max_workers: usize) -> Self {
        self.max_workers = Some(max_workers);
        self
    }

    /// Create a stream that reads events from the backend.
    ///
    /// This stream reads from the events stream and yields ActivityEvent items.
    /// It's designed to be used directly with SSE endpoints.
    pub fn event_stream(&self) -> impl Stream<Item = Result<ActivityEvent, WorkerError>> {
        let backend_stream = self.backend.event_stream();
        backend_stream.map(|r| {
            r.map(Self::convert_backend_event)
                .map_err(WorkerError::from)
        })
    }

    /// List pending activities.
    pub async fn list_pending(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let snapshots = self
            .backend
            .list_pending(offset, limit)
            .await
            .map_err(WorkerError::from)?;
        Ok(snapshots
            .into_iter()
            .map(Self::convert_backend_snapshot)
            .collect())
    }

    /// List processing activities.
    pub async fn list_processing(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let snapshots = self
            .backend
            .list_processing(offset, limit)
            .await
            .map_err(WorkerError::from)?;
        Ok(snapshots
            .into_iter()
            .map(Self::convert_backend_snapshot)
            .collect())
    }

    /// List scheduled activities.
    pub async fn list_scheduled(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let snapshots = self
            .backend
            .list_scheduled(offset, limit)
            .await
            .map_err(WorkerError::from)?;
        Ok(snapshots
            .into_iter()
            .map(Self::convert_backend_snapshot)
            .collect())
    }

    /// List dead letter activities.
    pub async fn list_dead_letter(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, WorkerError> {
        let records = self
            .backend
            .list_dead_letter(offset, limit)
            .await
            .map_err(WorkerError::from)?;
        Ok(records
            .into_iter()
            .map(Self::convert_backend_dead_letter)
            .collect())
    }

    /// List completed activities.
    pub async fn list_completed(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, WorkerError> {
        let snapshots = self
            .backend
            .list_completed(offset, limit)
            .await
            .map_err(WorkerError::from)?;
        Ok(snapshots
            .into_iter()
            .map(Self::convert_backend_snapshot)
            .collect())
    }

    /// Get a specific activity by ID.
    pub async fn get_activity(
        &self,
        activity_id: Uuid,
    ) -> Result<Option<ActivitySnapshot>, WorkerError> {
        self.backend
            .get_activity(activity_id)
            .await
            .map(|opt| opt.map(Self::convert_backend_snapshot))
            .map_err(WorkerError::from)
    }

    /// Get the result of a specific activity.
    pub async fn get_result(
        &self,
        activity_id: Uuid,
    ) -> Result<Option<serde_json::Value>, WorkerError> {
        self.backend
            .get_result(activity_id)
            .await
            .map(|opt| opt.and_then(|r| r.data))
            .map_err(WorkerError::from)
    }

    /// Get recent events for a specific activity.
    pub async fn recent_events(
        &self,
        activity_id: Uuid,
        limit: usize,
    ) -> Result<Vec<ActivityEvent>, WorkerError> {
        let events = self
            .backend
            .get_activity_events(activity_id, limit)
            .await
            .map_err(WorkerError::from)?;
        Ok(events
            .into_iter()
            .map(Self::convert_backend_event)
            .collect())
    }

    /// Get queue statistics.
    pub async fn stats(&self) -> Result<QueueStats, WorkerError> {
        let backend_stats = self.backend.stats().await.map_err(WorkerError::from)?;
        Ok(QueueStats {
            pending_activities: backend_stats.pending,
            processing_activities: backend_stats.processing,
            critical_priority: backend_stats.by_priority.critical,
            high_priority: backend_stats.by_priority.high,
            normal_priority: backend_stats.by_priority.normal,
            low_priority: backend_stats.by_priority.low,
            scheduled_activities: backend_stats.scheduled,
            dead_letter_activities: backend_stats.dead_letter,
            max_workers: self.max_workers.or(backend_stats.max_workers),
        })
    }

    /// Convert a backend ActivitySnapshot to a queue ActivitySnapshot
    fn convert_backend_snapshot(snapshot: crate::storage::ActivitySnapshot) -> ActivitySnapshot {
        ActivitySnapshot {
            id: snapshot.id,
            activity_type: snapshot.activity_type,
            payload: snapshot.payload,
            priority: snapshot.priority,
            status: snapshot.status,
            created_at: snapshot.created_at,
            scheduled_at: snapshot.scheduled_at,
            started_at: snapshot.started_at,
            completed_at: snapshot.completed_at,
            current_worker_id: snapshot.current_worker_id,
            last_worker_id: snapshot.last_worker_id,
            retry_count: snapshot.retry_count,
            max_retries: snapshot.max_retries,
            timeout_seconds: snapshot.timeout_seconds,
            retry_delay_seconds: snapshot.retry_delay_seconds,
            metadata: snapshot.metadata,
            last_error: snapshot.last_error,
            last_error_at: snapshot.last_error_at,
            status_updated_at: snapshot.status_updated_at,
            score: None,
            lease_deadline_ms: None,
            processing_member: None,
            idempotency_key: snapshot.idempotency_key,
        }
    }

    /// Convert a backend DeadLetterRecord to a local DeadLetterRecord
    fn convert_backend_dead_letter(record: DeadLetterRecord) -> DeadLetterRecord {
        DeadLetterRecord {
            activity: Self::convert_backend_snapshot(record.activity),
            error: record.error,
            failed_at: record.failed_at,
        }
    }

    /// Convert a backend ActivityEvent to a queue ActivityEvent.
    /// Since both backend and observability use the same types, this is now a simple passthrough.
    fn convert_backend_event(event: ActivityEvent) -> ActivityEvent {
        // The backend re-exports ActivityEvent from observability, so types are identical
        event
    }
}
