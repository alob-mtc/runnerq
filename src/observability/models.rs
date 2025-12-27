//! Observability models for queue inspection and monitoring.
//!
//! This module contains the core data types used for observability operations:
//! - [`ActivitySnapshot`]: A rich view of an activity's current state
//! - [`ActivityEvent`]: An event in an activity's lifecycle
//! - [`ActivityEventType`]: Types of lifecycle events
//! - [`DeadLetterRecord`]: A record from the dead letter queue

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::activity::activity::ActivityStatus;
use crate::ActivityPriority;

// ============================================================================
// ActivitySnapshot
// ============================================================================

/// A snapshot of an activity's current state.
///
/// This is a rich view of an activity used for observability and inspection.
/// It contains all the information needed to display activity details in UIs
/// and monitoring dashboards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivitySnapshot {
    /// Unique identifier for the activity
    pub id: Uuid,
    /// Activity type name (matches handler registration)
    pub activity_type: String,
    /// JSON payload passed to the handler
    pub payload: Value,
    /// Priority level for execution ordering
    pub priority: ActivityPriority,
    /// Current status of the activity
    pub status: ActivityStatus,
    /// When the activity was created
    pub created_at: DateTime<Utc>,
    /// When the activity is scheduled to run (if delayed)
    pub scheduled_at: Option<DateTime<Utc>>,
    /// When the activity started processing
    pub started_at: Option<DateTime<Utc>>,
    /// When the activity completed (success or failure)
    pub completed_at: Option<DateTime<Utc>>,
    /// Worker currently processing this activity
    pub current_worker_id: Option<String>,
    /// Last worker that processed this activity
    pub last_worker_id: Option<String>,
    /// Current retry attempt count
    pub retry_count: u32,
    /// Maximum allowed retries
    pub max_retries: u32,
    /// Timeout in seconds for activity execution
    pub timeout_seconds: u64,
    /// Base delay between retries in seconds
    pub retry_delay_seconds: u64,
    /// Custom metadata attached to the activity
    pub metadata: HashMap<String, String>,
    /// Last error message if failed
    pub last_error: Option<String>,
    /// When the last error occurred
    pub last_error_at: Option<DateTime<Utc>>,
    /// When the status was last updated
    pub status_updated_at: DateTime<Utc>,
    /// Redis ZSET score (for internal use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    /// Lease deadline timestamp in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_deadline_ms: Option<i64>,
    /// Processing member identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_member: Option<String>,
    /// Idempotency key if set
    pub idempotency_key: Option<String>,
}

impl ActivitySnapshot {
    /// Update the status and timestamp.
    pub fn update_status(&mut self, status: ActivityStatus, timestamp: DateTime<Utc>) {
        self.status = status;
        self.status_updated_at = timestamp;
    }

    /// Mark the activity as started.
    pub fn mark_started(&mut self, started_at: DateTime<Utc>) {
        self.started_at = Some(started_at);
    }

    /// Mark the activity as completed.
    pub fn mark_completed(&mut self, completed_at: DateTime<Utc>) {
        self.completed_at = Some(completed_at);
    }

    /// Set the current worker ID.
    pub fn set_current_worker_id(&mut self, worker_id: Option<String>) {
        self.current_worker_id = worker_id;
    }

    /// Set the last worker ID.
    pub fn set_last_worker_id(&mut self, worker_id: Option<String>) {
        self.last_worker_id = worker_id;
    }

    /// Set the last error information.
    pub fn set_last_error(&mut self, error: Option<String>, at: Option<DateTime<Utc>>) {
        self.last_error = error;
        self.last_error_at = at;
    }
}

// ============================================================================
// ActivityEventType
// ============================================================================

/// Types of lifecycle events for activities.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ActivityEventType {
    /// Activity was added to the queue
    Enqueued,
    /// Activity was scheduled for future execution
    Scheduled,
    /// Activity was removed from the queue by a worker
    Dequeued,
    /// Activity processing started
    Started,
    /// Activity completed successfully
    Completed,
    /// Activity failed (may retry)
    Failed,
    /// Activity is being retried
    Retrying,
    /// Activity was moved to dead letter queue
    DeadLetter,
    /// Activity was requeued (e.g., after lease expiry)
    Requeued,
    /// Activity lease was extended
    LeaseExtended,
    /// Activity result was stored
    ResultStored,
}

// ============================================================================
// ActivityEvent
// ============================================================================

/// An event in an activity's lifecycle.
///
/// These events are recorded for auditing and debugging purposes,
/// and can be streamed for real-time monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEvent {
    /// ID of the activity this event belongs to
    pub activity_id: Uuid,
    /// When the event occurred
    pub timestamp: DateTime<Utc>,
    /// Type of event
    pub event_type: ActivityEventType,
    /// Worker that triggered the event (if applicable)
    pub worker_id: Option<String>,
    /// Additional event details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

// ============================================================================
// DeadLetterRecord
// ============================================================================

/// A record from the dead letter queue.
///
/// Contains the activity snapshot along with failure information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    /// The activity that failed
    pub activity: ActivitySnapshot,
    /// The error message
    pub error: String,
    /// When the activity was moved to DLQ
    pub failed_at: DateTime<Utc>,
}

// ============================================================================
// QueueStats
// ============================================================================

/// Statistics about queue state.
///
/// Used for monitoring and dashboard displays.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueueStats {
    /// Number of activities waiting to be processed
    pub pending_activities: u64,
    /// Number of activities currently being processed
    pub processing_activities: u64,
    /// Number of critical priority activities
    pub critical_priority: u64,
    /// Number of high priority activities
    pub high_priority: u64,
    /// Number of normal priority activities
    pub normal_priority: u64,
    /// Number of low priority activities
    pub low_priority: u64,
    /// Number of activities scheduled for future execution
    pub scheduled_activities: u64,
    /// Number of activities in the dead letter queue
    pub dead_letter_activities: u64,
    /// Maximum configured workers
    pub max_workers: Option<usize>,
}
