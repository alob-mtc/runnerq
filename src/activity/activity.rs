use async_trait::async_trait;

use crate::queue::queue::{ActivityQueueTrait, ResultState};
use crate::runner::runner::ActivityExecutor;
use crate::WorkerError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
// Import error handling types
use super::error::ActivityError;

/// Activity priority levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActivityPriority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl Default for ActivityPriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// Activity status tracking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ActivityStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
    DeadLetter,
}

pub struct ActivityOption {
    pub priority: Option<ActivityPriority>,
    pub max_retries: u32,
    pub timeout_seconds: u64,
    pub scheduled_at: Option<u64>,
}

/// Represents an Activity to be processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Activity {
    pub id: Uuid,
    pub activity_type: String,
    pub payload: serde_json::Value,
    pub priority: ActivityPriority,
    pub status: ActivityStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub timeout_seconds: u64,
    pub retry_delay_seconds: u64,
    pub metadata: HashMap<String, String>,
}

impl Activity {
    pub fn new(
        activity_type: String,
        payload: serde_json::Value,
        option: Option<ActivityOption>,
    ) -> Self {
        let (priority, max_retries, timeout_seconds, scheduled_at) = if let Some(opt) = option {
            (
                opt.priority.unwrap_or(ActivityPriority::default()),
                opt.max_retries,
                opt.timeout_seconds,
                opt.scheduled_at,
            )
        } else {
            (ActivityPriority::default(), 3, 300, None)
        };

        Self {
            id: Uuid::new_v4(),
            activity_type,
            payload,
            priority,
            status: ActivityStatus::Pending,
            created_at: chrono::Utc::now(),
            scheduled_at: scheduled_at.map(|timestamp| {
                chrono::DateTime::from_timestamp(timestamp as i64, 0)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            retry_count: 0,
            max_retries,
            timeout_seconds,
            retry_delay_seconds: 1,
            metadata: HashMap::new(),
        }
    }
}

/// Context provided to Activity handlers during execution
#[derive(Clone)]
pub struct ActivityContext {
    pub activity_id: Uuid,
    pub activity_type: String,
    pub retry_count: u32,
    pub metadata: HashMap<String, String>,
    pub worker_engine: Arc<dyn ActivityExecutor>,
}

///A convenient Result type alias for use in activity handlers that want to use ? operator
pub type ActivityHandlerResult<T = Option<serde_json::Value>> = Result<T, ActivityError>;

/// Trait that all Activity handlers must implement
#[async_trait]
pub trait ActivityHandler: Send + Sync {
    async fn handle(
        &self,
        payload: serde_json::Value,
        context: ActivityContext,
    ) -> ActivityHandlerResult;

    fn activity_type(&self) -> String;
}

/// Registry for Activity handlers
pub(crate) type ActivityHandlerRegistry = HashMap<String, Arc<dyn ActivityHandler>>;

pub struct ActivityFuture {
    queue: Arc<dyn ActivityQueueTrait>,
    activity_id: Uuid,
}

impl ActivityFuture {
    /// Creates a new ActivityFuture that can be used to poll for the result of a specific activity.
    ///
    /// The returned ActivityFuture holds the queue used to retrieve results and the UUID of the activity to query.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use uuid::Uuid;
    /// # use crate::ActivityFuture;
    /// # let queue = Arc::new(/* an implementation of ActivityQueueTrait */);
    /// let activity_id = Uuid::new_v4();
    /// let fut = ActivityFuture::new(queue, activity_id);
    /// ```
    pub(crate) fn new(queue: Arc<dyn ActivityQueueTrait>, activity_id: Uuid) -> Self {
        Self { queue, activity_id }
    }

    /// Waits for and returns the completed activity result, consuming the `ActivityFuture`.
    ///
    /// This async method polls the associated activity queue until the activity produces a result
    /// or a 5-minute timeout elapses. On success it returns `Ok(Some(value))` when the activity
    /// completed with a value, or `Ok(None)` when it completed without a value. If the activity
    /// failed, the failure payload is converted to a JSON string and returned as
    /// `Err(WorkerError::CustomError)`. If no result arrives within 5 minutes, it returns
    /// `Err(WorkerError::Timeout)`.
    ///
    /// The function propagates errors returned by the queue (e.g., `WorkerError` variants)
    /// encountered while polling.
    ///
    /// # Examples
    ///
    /// ```rust
    /// // given `queue: Arc<dyn ActivityQueueTrait>` and `activity_id: Uuid`
    /// let fut = ActivityFuture::new(queue.clone(), activity_id);
    /// match fut.get_result().await {
    ///     Ok(Some(json)) => println!("activity result: {:?}", json),
    ///     Ok(None) => println!("activity completed with no result"),
    ///     Err(e) => eprintln!("activity failed or timed out: {:?}", e),
    /// }
    /// ```
    pub async fn get_result(self) -> Result<Option<serde_json::Value>, crate::WorkerError> {
        let timeout = std::time::Duration::from_secs(300); // 5 minutes timeout

        tokio::time::timeout(timeout, async move {
            loop {
                if let Some(result) = self.queue.get_result(self.activity_id).await? {
                    return match result.state {
                        ResultState::Ok => Ok(result.data),
                        ResultState::Err => {
                            let result_json = serde_json::to_string(&result.data)?;
                            Err(WorkerError::CustomError(result_json))
                        }
                    };
                }

                // Use exponential backoff to reduce load: start with 50ms, cap at 1s
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| crate::WorkerError::Timeout)?
    }
}
