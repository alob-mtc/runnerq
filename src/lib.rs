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
//! use runner_q::{WorkerEngine, ActivityPriority, ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
//! use std::sync::Arc;
//! use async_trait::async_trait;
//! use serde_json::json;
//! use serde::{Serialize, Deserialize};
//! use std::time::Duration;
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
//!     // Improved API: Builder pattern for WorkerEngine
//!     let engine = WorkerEngine::builder()
//!         .redis_url("redis://localhost:6379")
//!         .queue_name("my_app")
//!         .max_workers(8)
//!         .schedule_poll_interval(Duration::from_secs(30))
//!         .build()
//!         .await?;
//!
//!     // Register activity handler
//!     let send_email_activity = SendEmailActivity;
//!     engine.register_activity(MyActivityType::SendEmail.to_string(), Arc::new(send_email_activity));
//!
//!     // Get activity executor for fluent activity execution
//!     let activity_executor = engine.get_activity_executor();
//!
//!     // Improved API: Fluent activity execution
//!     let future = activity_executor
//!         .activity("send_email")
//!         .payload(json!({"to": "user@example.com", "subject": "Welcome!"}))
//!         .max_retries(5)
//!         .timeout(Duration::from_secs(600))
//!         .execute()
//!         .await?;
//!
//!     // Schedule an activity for future execution (10 seconds from now)
//!     let scheduled_future = activity_executor
//!         .activity("send_email")
//!         .payload(json!({"to": "user@example.com", "subject": "Reminder"}))
//!         .max_retries(3)
//!         .timeout(Duration::from_secs(300))
//!         .delay(Duration::from_secs(10))
//!         .execute()
//!         .await?;
//!
//!     // Execute an activity with default options
//!     let future2 = activity_executor
//!         .activity("send_email")
//!         .payload(json!({"to": "admin@example.com"}))
//!         .execute()
//!         .await?;
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
//!     engine.start().await?;
//!
//!     Ok(())
//! }
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
//!         let _payment_future = context.activity_executor
//!             .activity("validate_payment")
//!             .payload(serde_json::json!({"order_id": order_id}))
//!             .priority(ActivityPriority::High)
//!             .max_retries(3)
//!             .timeout(std::time::Duration::from_secs(120))
//!             .execute()
//!             .await.map_err(|e| ActivityError::Retry(format!("Failed to enqueue payment validation: {}", e)))?;
//!         
//!         // Step 2: Update inventory
//!         let _inventory_future = context.activity_executor
//!             .activity("update_inventory")
//!             .payload(serde_json::json!({"order_id": order_id}))
//!             .execute()
//!             .await.map_err(|e| ActivityError::Retry(format!("Failed to enqueue inventory update: {}", e)))?;
//!         
//!         // Step 3: Schedule delivery notification for later
//!         context.activity_executor
//!             .activity("send_delivery_notification")
//!             .payload(serde_json::json!({"order_id": order_id, "customer_email": payload["customer_email"]}))
//!             .max_retries(5)
//!             .timeout(std::time::Duration::from_secs(300))
//!             .delay(std::time::Duration::from_secs(3600)) // 1 hour
//!             .execute()
//!             .await.map_err(|e| ActivityError::Retry(format!("Failed to schedule notification: {}", e)))?;
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
pub use crate::runner::redis::RedisConfig;
pub use crate::runner::runner::{
    ActivityBuilder, ActivityExecutor, MetricsSink, WorkerEngine, WorkerEngineBuilder,
};
pub use activity::activity::{
    ActivityContext, ActivityFuture, ActivityHandler, ActivityHandlerResult, ActivityPriority,
};
pub use activity::error::{ActivityError, RetryableError};
