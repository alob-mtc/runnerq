use async_trait::async_trait;

use crate::queue::queue::{ActivityQueueTrait, ResultState};
use crate::runner::runner::ActivityExecutor;
use crate::WorkerError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
// Import error handling types
use super::error::ActivityError;

/// Activity priority levels that determine the order of execution.
///
/// Activities with higher priority are processed before those with lower priority.
/// Within the same priority level, activities are processed in FIFO (first-in, first-out) order.
///
/// # Priority Ordering
///
/// 1. **Critical** - Highest priority, processed first
/// 2. **High** - High priority activities
/// 3. **Normal** - Default priority for most activities
/// 4. **Low** - Lowest priority, processed last
///
/// # Examples
///
/// ```rust
/// use runner_q::ActivityPriority;
/// use runner_q::ActivityOption;
///
/// // Create a high-priority activity
/// let options = ActivityOption {
///     priority: Some(ActivityPriority::High),
///     max_retries: 3,
///     timeout_seconds: 300,
///     delay_seconds: None,
/// };
///
/// // Critical priority for urgent tasks
/// let critical_options = ActivityOption {
///     priority: Some(ActivityPriority::Critical),
///     max_retries: 5,
///     timeout_seconds: 600,
///     delay_seconds: None,
/// };
/// ```
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActivityPriority {
    /// Low priority - processed after all higher priority activities
    Low = 1,
    /// Normal priority - default priority for most activities
    #[default]
    Normal = 2,
    /// High priority - processed before Normal and Low priority activities
    High = 3,
    /// Critical priority - highest priority, processed first
    Critical = 4,
}


/// Activity status tracking throughout the activity lifecycle.
///
/// Activities progress through these states as they are processed by the worker engine.
/// The status is used for monitoring, debugging, and understanding the current state of activities.
///
/// # Lifecycle Flow
///
/// ```
/// Pending -> Running -> Completed
///    |         |
/// Retrying -> Failed -> DeadLetter
/// ```
///
/// # Examples
///
/// ```rust
/// use runner_q::ActivityStatus;
///
/// // Check if an activity is in a terminal state
/// fn is_terminal_status(status: &ActivityStatus) -> bool {
///     matches!(status, ActivityStatus::Completed | ActivityStatus::Failed | ActivityStatus::DeadLetter)
/// }
///
/// // Check if an activity is currently being processed
/// fn is_active_status(status: &ActivityStatus) -> bool {
///     matches!(status, ActivityStatus::Pending | ActivityStatus::Running | ActivityStatus::Retrying)
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ActivityStatus {
    /// Activity is queued and waiting to be processed
    Pending,
    /// Activity is currently being executed by a worker
    Running,
    /// Activity completed successfully
    Completed,
    /// Activity failed and will not be retried
    Failed,
    /// Activity failed but is scheduled for retry
    Retrying,
    /// Activity exceeded maximum retry attempts and moved to dead letter queue
    DeadLetter,
}

pub struct ActivityOption {
    /// Priority level for the activity.
    ///
    /// When `None`, uses the default priority (Normal).
    /// Higher priority activities are processed before lower priority ones.
    pub priority: Option<ActivityPriority>,
    
    /// Maximum number of retry attempts for failed activities.
    ///
    /// Set to `0` for unlimited retries (use with caution).
    /// When retries are exhausted, the activity is moved to the dead letter queue.
    pub max_retries: u32,
    
    /// Maximum execution time in seconds before the activity times out.
    ///
    /// Timed out activities are automatically retried unless max_retries is exceeded.
    pub timeout_seconds: u64,
    
    /// Delay in seconds before the activity should be executed.
    ///
    /// When `None`, the activity is executed immediately.
    /// When `Some(seconds)`, the activity is scheduled for future execution.
    pub delay_seconds: Option<u64>,
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
    pub(crate) fn new(
        activity_type: String,
        payload: serde_json::Value,
        option: Option<ActivityOption>,
    ) -> Self {
        let (priority, max_retries, timeout_seconds, delay_seconds) = if let Some(opt) = option {
            (
                opt.priority.unwrap_or(ActivityPriority::default()),
                opt.max_retries,
                opt.timeout_seconds,
                opt.delay_seconds,
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
            scheduled_at: delay_seconds.map(|delay_seconds| {
                chrono::Utc::now() + chrono::Duration::seconds(delay_seconds as i64)
            }),
            retry_count: 0,
            max_retries,
            timeout_seconds,
            retry_delay_seconds: 1,
            metadata: HashMap::new(),
        }
    }
}

/// Context provided to Activity handlers during execution.
///
/// This struct contains all the information and utilities available to activity handlers
/// during their execution, including metadata, retry information, and the ability to execute
/// other activities for orchestration.
///
/// # Examples
///
/// ```rust
/// use runner_q::{ActivityContext, ActivityHandler, ActivityHandlerResult, ActivityOption, ActivityPriority};
/// use async_trait::async_trait;
/// use serde_json::Value;
///
/// pub struct OrderProcessingHandler;
///
/// #[async_trait]
/// impl ActivityHandler for OrderProcessingHandler {
///     async fn handle(&self, payload: Value, context: ActivityContext) -> ActivityHandlerResult {
///         // Access activity metadata
///         println!("Processing activity {} of type {}", context.activity_id, context.activity_type);
///         println!("This is retry attempt #{}", context.retry_count);
///         
///         // Execute a follow-up activity
///         let follow_up_future = context.activity_executor.execute_activity(
///             "send_confirmation_email".to_string(),
///             serde_json::json!({"order_id": payload["order_id"]}),
///             Some(ActivityOption {
///                 priority: Some(ActivityPriority::Normal),
///                 max_retries: 3,
///                 timeout_seconds: 300,
///                 delay_seconds: None,
///             })
///         ).await?;
///         
///         // Check for cancellation
///         if context.cancel_token.is_cancelled() {
///             return Err(ActivityError::NonRetry("Activity was cancelled".to_string()));
///         }
///         
///         Ok(Some(serde_json::json!({"status": "processed"})))
///     }
///     
///     fn activity_type(&self) -> String {
///         "process_order".to_string()
///     }
/// }
/// ```
#[derive(Clone)]
pub struct ActivityContext {
    /// Unique identifier for this activity instance
    pub activity_id: Uuid,
    /// Type of activity being executed
    pub activity_type: String,
    /// Current retry attempt number (0 for first execution)
    pub retry_count: u32,
    /// Custom metadata associated with the activity
    pub metadata: HashMap<String, String>,
    /// Token for checking if the activity has been cancelled
    pub cancel_token: CancellationToken,
    /// Reference to the worker engine for executing other activities
    pub activity_executor: Arc<dyn ActivityExecutor>,
}

///A convenient Result type alias for use in activity handlers that want to use ? operator
pub type ActivityHandlerResult<T = Option<serde_json::Value>> = Result<T, ActivityError>;

/// Trait that all Activity handlers must implement.
///
/// This trait defines the interface for processing activities in the worker engine.
/// Implementations should be stateless and thread-safe, as they may be called
/// concurrently by multiple worker threads.
///
/// # Examples
///
/// ```rust
/// use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
/// use async_trait::async_trait;
/// use serde_json::Value;
///
/// pub struct EmailHandler {
///     smtp_client: SmtpClient, // Your SMTP client
/// }
///
/// #[async_trait]
/// impl ActivityHandler for EmailHandler {
///     async fn handle(&self, payload: Value, context: ActivityContext) -> ActivityHandlerResult {
///         // Extract email data from payload
///         let to = payload["to"]
///             .as_str()
///             .ok_or_else(|| ActivityError::NonRetry("Missing 'to' field".to_string()))?;
///         
///         let subject = payload["subject"]
///             .as_str()
///             .unwrap_or("No Subject");
///         
///         // Send email (your implementation)
///         match self.smtp_client.send_email(to, subject, &payload["body"]).await {
///             Ok(message_id) => Ok(Some(serde_json::json!({
///                 "message_id": message_id,
///                 "status": "sent"
///             }))),
///             Err(e) if e.is_retryable() => Err(ActivityError::Retry(e.to_string())),
///             Err(e) => Err(ActivityError::NonRetry(e.to_string())),
///         }
///     }
///     
///     fn activity_type(&self) -> String {
///         "send_email".to_string()
///     }
/// }
/// ```
#[async_trait]
pub trait ActivityHandler: Send + Sync {
    /// Process the activity with the given payload and context.
    ///
    /// This method is called by the worker engine to execute the activity.
    /// It should return:
    /// - `Ok(Some(value))` - Activity completed successfully with result data
    /// - `Ok(None)` - Activity completed successfully with no result data
    /// - `Err(ActivityError::Retry(reason))` - Activity failed but should be retried
    /// - `Err(ActivityError::NonRetry(reason))` - Activity failed and should not be retried
    ///
    /// # Parameters
    ///
    /// * `payload` - JSON payload containing the activity data
    /// * `context` - Execution context with metadata and utilities
    ///
    /// # Returns
    ///
    /// Returns a result indicating success or failure, with optional retry behavior.
    async fn handle(
        &self,
        payload: serde_json::Value,
        context: ActivityContext,
    ) -> ActivityHandlerResult;

    /// Return the activity type string that this handler processes.
    ///
    /// This string is used to match activities with their handlers when they are
    /// registered with the worker engine. It should be unique and descriptive.
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
    /// # use runner_q::ActivityFuture;
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
