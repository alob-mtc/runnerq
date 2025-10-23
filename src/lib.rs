//! Runner-Q: A Redis-based activity queue and worker system for Rust
//!
//! This crate provides a robust, scalable activity queue system built on Redis with support for:
//! - **Priority-based activity processing** with Critical, High, Normal, and Low priority levels
//! - **Activity scheduling** with precise timestamp-based scheduling for future execution
//! - **Intelligent retry mechanism** with exponential backoff for failed activities
//! - **Dead letter queue** handling for activities that exceed retry limits
//! - **Concurrent activity processing** with configurable worker pools
//! - **Graceful shutdown** handling with proper cleanup
//! - **Activity orchestration** enabling activities to execute other activities
//! - **Comprehensive error handling** with retryable and non-retryable error types
//! - **Activity metadata** support for context and tracking
//! - **Redis persistence** for durability and scalability
//! - **Queue statistics** and monitoring capabilities
//!
//! # Example
//!
//! ```rust,no_run
//! use runner_q::{ActivityQueue, WorkerEngine, ActivityPriority, ActivityOption};
//! use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
//! use runner_q::config::WorkerConfig;
//! use std::sync::Arc;
//! use async_trait::async_trait;
//! use serde::{Serialize, Deserialize};
//! use serde_json::Value;
//!
//! // Define activity types
//! #[derive(Debug, Clone)]
//! enum MyActivityType {
//!     SendEmail,
//!     ProcessPayment,
//! }
//!
//! impl std::fmt::Display for MyActivityType {
//!     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//!         match self {
//!             MyActivityType::SendEmail => write!(f, "send_email"),
//!             MyActivityType::ProcessPayment => write!(f, "process_payment"),
//!         }
//!     }
//! }
//!
//! // Implement activity handler
//! pub struct SendEmailActivity;
//!
//! #[async_trait]
//! impl ActivityHandler for SendEmailActivity {
//!     async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
//!         // Parse the email data - use ? operator for clean error handling
//!         let email_data: serde_json::Map<String, serde_json::Value> = payload
//!             .as_object()
//!             .ok_or_else(|| ActivityError::NonRetry("Invalid payload format".to_string()))?
//!             .clone();
//!         
//!         let to = email_data.get("to")
//!             .and_then(|v| v.as_str())
//!             .ok_or_else(|| ActivityError::NonRetry("Missing 'to' field".to_string()))?;
//!         
//!         // Simulate sending email
//!         println!("Sending email to: {}", to);
//!         
//!         // Return success with result data
//!         Ok(Some(serde_json::json!({
//!             "message": format!("Email sent to {}", to),
//!             "status": "delivered"
//!         })))
//!     }
//!
//!     fn activity_type(&self) -> String {
//!         MyActivityType::SendEmail.to_string()
//!     }
//! }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! pub struct EmailResult {
//!     message: String,
//!     status: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create Redis connection pool
//!     let redis_pool = bb8_redis::bb8::Pool::builder()
//!         .build(bb8_redis::RedisConnectionManager::new("redis://127.0.0.1:6379")?)
//!         .await?;
//!
//!     // Create worker engine
//!     let config = WorkerConfig::default();
//!     let mut worker_engine = WorkerEngine::new(redis_pool, config);
//!
//!     // Register activity handler
//!     let send_email_activity = SendEmailActivity;
//!     worker_engine.register_activity(MyActivityType::SendEmail.to_string(), Arc::new(send_email_activity));
//!
//!     // Execute an activity with custom options
//!     let future = worker_engine.execute_activity(
//!         MyActivityType::SendEmail.to_string(),
//!         serde_json::json!({"to": "user@example.com", "subject": "Welcome!"}),
//!         Some(ActivityOption {
//!             priority: Some(ActivityPriority::High),
//!             max_retries: 5,
//!             timeout_seconds: 600, // 10 minutes
//!             delay_seconds: None, // Execute immediately
//!         })
//!     ).await?;
//!
//!     // Schedule an activity for future execution (10 seconds from now)
//!     let delay_seconds = 10;
//!     let scheduled_future = worker_engine.execute_activity(
//!         MyActivityType::SendEmail.to_string(),
//!         serde_json::json!({"to": "user@example.com", "subject": "Reminder"}),
//!         Some(ActivityOption {
//!             priority: Some(ActivityPriority::Normal),
//!             max_retries: 3,
//!             timeout_seconds: 300,
//!             delay_seconds: Some(delay_seconds as u64),
//!         })
//!     ).await?;
//!
//!     // Execute an activity with default options
//!     let future2 = worker_engine.execute_activity(
//!         MyActivityType::SendEmail.to_string(),
//!         serde_json::json!({"to": "admin@example.com"}),
//!         None // Uses default priority (Normal), 3 retries, 300s timeout, immediate execution
//!     ).await?;
//!
//!     // Spawn a task to handle the result
//!     tokio::spawn(async move {
//!         if let Ok(result) = future.get_result().await {
//!             match result {
//!                 None => {}
//!                 Some(data) => {
//!                     let email_result: EmailResult = serde_json::from_value(data).unwrap();
//!                     println!("Email result: {:?}", email_result);
//!                 }
//!             }
//!         }
//!     });
//!
//!     // Start the worker engine (this will run indefinitely)
//!     worker_engine.start().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Activity Scheduling
//!
//! Runner-Q supports precise scheduling of activities for future execution using Unix timestamps.
//! Scheduled activities are stored in a Redis sorted set and automatically processed when their
//! scheduled time arrives.
//!
//! ```rust,no_run
//! use runner_q::{WorkerEngine, ActivityOption, ActivityPriority};
//! use chrono::{Utc, Duration};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let worker_engine: WorkerEngine = todo!();
//! // Schedule an activity for 30 minutes from now
//! let scheduled_time = (Utc::now() + Duration::minutes(30)).timestamp() as u64;
//!
//! let future = worker_engine.execute_activity(
//!     "send_reminder".to_string(),
//!     serde_json::json!({"user_id": 123, "message": "Don't forget your appointment!"}),
//!     Some(ActivityOption {
//!         priority: Some(ActivityPriority::Normal),
//!         max_retries: 3,
//!         timeout_seconds: 300,
//!         delay_seconds: Some(scheduled_time),
//!     })
//! ).await?;
//!
//! // Schedule a recurring task (daily report at 9 AM)
//! let tomorrow_9am = Utc::now()
//!     .date_naive()
//!     .and_hms_opt(9, 0, 0).unwrap()
//!     .and_utc()
//!     + Duration::days(1);
//!
//! worker_engine.execute_activity(
//!     "generate_daily_report".to_string(),
//!     serde_json::json!({"report_type": "daily", "date": tomorrow_9am.format("%Y-%m-%d").to_string()}),
//!     Some(ActivityOption {
//!         priority: Some(ActivityPriority::High),
//!         max_retries: 5,
//!         timeout_seconds: 1800, // 30 minutes
//!         delay_seconds: Some(tomorrow_9am.timestamp() as u64),
//!     })
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Activity Orchestration
//!
//! Activities can execute other activities using the `ActivityExecutor` available in the
//! `ActivityContext`. This enables building complex workflows and activity orchestration patterns.
//!
//! ```rust,no_run
//! use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityOption, ActivityPriority, ActivityError};
//! use async_trait::async_trait;
//!
//! pub struct OrderProcessingActivity;
//!
//! #[async_trait]
//! impl ActivityHandler for OrderProcessingActivity {
//!     async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
//!         let order_id = payload["order_id"]
//!             .as_str()
//!             .ok_or_else(|| ActivityError::NonRetry("Missing order_id".to_string()))?;
//!         
//!         // Step 1: Validate payment
//!         let _payment_future = context.worker_engine.execute_activity(
//!             "validate_payment".to_string(),
//!             serde_json::json!({"order_id": order_id}),
//!             Some(ActivityOption {
//!                 priority: Some(ActivityPriority::High),
//!                 max_retries: 3,
//!                 timeout_seconds: 120,
//!                 delay_seconds: None,
//!             })
//!         ).await.map_err(|e| ActivityError::Retry(format!("Failed to enqueue payment validation: {}", e)))?;
//!         
//!         // Step 2: Update inventory
//!         let _inventory_future = context.worker_engine.execute_activity(
//!             "update_inventory".to_string(),
//!             serde_json::json!({"order_id": order_id}),
//!             None
//!         ).await.map_err(|e| ActivityError::Retry(format!("Failed to enqueue inventory update: {}", e)))?;
//!         
//!         // Step 3: Schedule delivery notification for later
//!         let notification_time = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as u64;
//!         context.worker_engine.execute_activity(
//!             "send_delivery_notification".to_string(),
//!             serde_json::json!({"order_id": order_id, "customer_email": payload["customer_email"]}),
//!             Some(ActivityOption {
//!                 priority: Some(ActivityPriority::Normal),
//!                 max_retries: 5,
//!                 timeout_seconds: 300,
//!                 delay_seconds: Some(notification_time),
//!             })
//!         ).await.map_err(|e| ActivityError::Retry(format!("Failed to schedule notification: {}", e)))?;
//!         
//!         Ok(Some(serde_json::json!({
//!             "order_id": order_id,
//!             "status": "processing",
//!             "steps_initiated": ["payment_validation", "inventory_update", "delivery_notification"]
//!         })))
//!     }
//!
//!     fn activity_type(&self) -> String {
//!         "process_order".to_string()
//!     }
//! }
//! ```

pub mod activity;
pub mod config;
pub mod queue;
pub mod runner;
pub mod worker;

// Re-export main types for easy access
pub use crate::config::WorkerConfig;
pub use crate::queue::queue::{ActivityQueue, QueueStats};
pub use crate::runner::error::WorkerError;
pub use crate::runner::runner::{ActivityExecutor, WorkerEngine};
pub use activity::activity::{
    ActivityContext, ActivityFuture, ActivityHandler, ActivityHandlerResult, ActivityOption,
    ActivityPriority, ActivityStatus,
};
pub use activity::error::{ActivityError, RetryableError};
