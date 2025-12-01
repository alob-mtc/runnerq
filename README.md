# Runner-Q

A robust, scalable Redis-based activity queue and worker system for Rust applications.

## Features

- **Priority-based activity processing** - Support for Critical, High, Normal, and Low priority levels
- **Activity scheduling** - Precise timestamp-based scheduling for future execution
- **Intelligent retry mechanism** - Built-in retry mechanism with exponential backoff
- **Dead letter queue** - Failed activities are moved to a dead letter queue for inspection
- **Concurrent activity processing** - Configurable number of concurrent workers
- **Graceful shutdown** - Proper shutdown handling with signal support
- **Activity orchestration** - Activities can execute other activities for complex workflows
- **Comprehensive error handling** - Retryable and non-retryable error types
- **Activity metadata** - Support for custom metadata on activities
- **Redis persistence** - Activities are stored in Redis for durability
- **Built-in observability console** - Real-time web UI for monitoring and managing activities
- **Queue statistics** - Monitoring capabilities and metrics collection

## Installation

```sh
cargo add runner_q
```

## Quick Start

```rust
use runner_q::{WorkerEngine, ActivityPriority, ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::json;
use serde::{Serialize, Deserialize};
use std::time::Duration;

// Define activity types
#[derive(Debug, Clone)]
enum MyActivityType {
    SendEmail,
    ProcessPayment,
}

impl std::fmt::Display for MyActivityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyActivityType::SendEmail => write!(f, "send_email"),
            MyActivityType::ProcessPayment => write!(f, "process_payment"),
        }
    }
}

// Implement activity handler
pub struct SendEmailActivity;

#[async_trait]
impl ActivityHandler for SendEmailActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
        // Parse the email data - use ? operator for clean error handling
        let email_data: serde_json::Map<String, serde_json::Value> = payload
            .as_object()
            .ok_or_else(|| ActivityError::NonRetry("Invalid payload format".to_string()))?
            .clone();
        
        let to = email_data.get("to")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ActivityError::NonRetry("Missing 'to' field".to_string()))?;
        
        // Simulate sending email
        println!("Sending email to: {}", to);
        
        // Return success with result data
        Ok(Some(serde_json::json!({
            "message": format!("Email sent to {}", to),
            "status": "delivered"
        })))
    }

    fn activity_type(&self) -> String {
        MyActivityType::SendEmail.to_string()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmailResult {
    message: String,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Improved API: Builder pattern for WorkerEngine
    let engine = WorkerEngine::builder()
        .redis_url("redis://localhost:6379")
        .queue_name("my_app")
        .max_workers(8)
        .schedule_poll_interval(Duration::from_secs(30))
        .build()
        .await?;

    // Register activity handler
    let send_email_activity = SendEmailActivity;
    engine.register_activity(MyActivityType::SendEmail.to_string(), Arc::new(send_email_activity));

    // Get activity executor for fluent activity execution
    // Note: You need to get the activity executor to use the fluent API
    let activity_executor = engine.get_activity_executor();
    
    // Execute an activity with custom options
    let future = activity_executor
        .activity("send_email")
        .payload(json!({"to": "user@example.com", "subject": "Welcome!"}))
        .max_retries(5)
        .timeout(Duration::from_secs(600))
        .execute()
        .await?;

    // Schedule an activity for future execution (10 seconds from now)
    let scheduled_future = activity_executor
        .activity("send_email")
        .payload(json!({"to": "user@example.com", "subject": "Reminder"}))
        .max_retries(3)
        .timeout(Duration::from_secs(300))
        .delay(Duration::from_secs(10))
        .execute()
        .await?;

    // Execute an activity with default options
    let future2 = activity_executor
        .activity("send_email")
        .payload(json!({"to": "admin@example.com"}))
        .execute()
        .await?;

    // Spawn a task to handle the result
    tokio::spawn(async move {
        if let Ok(result) = future.get_result().await {
            match result {
                None => {}
                Some(data) => {
                    let email_result: EmailResult = serde_json::from_value(data).unwrap();
                    println!("Email result: {:?}", email_result);
                }
            }
        }
    });

    // Start the worker engine (this will run indefinitely)
    engine.start().await?;

    Ok(())
}
```

## Builder Pattern API

Runner-Q provides a fluent builder pattern for both `WorkerEngine` configuration and activity execution, making the API more ergonomic and easier to use.

### WorkerEngine Builder

```rust
use runner_q::WorkerEngine;
use std::time::Duration;

// Basic configuration
let engine = WorkerEngine::builder()
    .redis_url("redis://localhost:6379")
    .queue_name("my_app")
    .max_workers(8)
    .schedule_poll_interval(Duration::from_secs(30))
    .build()
    .await?;

// Advanced configuration with Redis config and metrics
use runner_q::{RedisConfig, MetricsSink};
use std::sync::Arc;

let redis_config = RedisConfig {
    max_size: 100,
    min_idle: 10,
    conn_timeout: Duration::from_secs(60),
    idle_timeout: Duration::from_secs(600),
    max_lifetime: Duration::from_secs(3600),
};

// Custom metrics implementation
struct PrometheusMetrics;
impl MetricsSink for PrometheusMetrics {
    fn inc_counter(&self, name: &str, value: u64) {
        println!("Counter {}: {}", name, value);
    }
    fn observe_duration(&self, name: &str, duration: Duration) {
        println!("Duration {}: {:?}", name, duration);
    }
}

let engine = WorkerEngine::builder()
    .redis_url("redis://localhost:6379")
    .queue_name("my_app")
    .max_workers(8)
    .redis_config(redis_config)
    .metrics(Arc::new(PrometheusMetrics))
    .build()
    .await?;
```

### Activity Builder

```rust
use runner_q::{WorkerEngine, ActivityPriority};
use serde_json::json;
use std::time::Duration;

// Get activity executor for fluent activity execution
// Note: The fluent API is available through the activity executor, not directly on the engine
let activity_executor = engine.get_activity_executor();

// Fluent activity execution
let future = activity_executor
    .activity("send_email")
    .payload(json!({"to": "user@example.com", "subject": "Hello"}))
    .max_retries(5)
    .timeout(Duration::from_secs(600))
    .execute()
    .await?;

// Schedule activity for future execution
let scheduled_future = activity_executor
    .activity("send_reminder")
    .payload(json!({"user_id": 123}))
    .delay(Duration::from_secs(3600)) // 1 hour delay
    .execute()
    .await?;

// Simple activity with defaults
let simple_future = activity_executor
    .activity("process_data")
    .payload(json!({"data": "example"}))
    .execute()
    .await?;
```

## Activity Types

Activity types in Runner-Q are simple strings that identify different types of activities. You can use any string as an activity type identifier.

### Examples

```rust
// Common activity types
"send_email"
"process_payment"
"provision_card"
"update_card_status"
"process_webhook_event"

// You can use any string format you prefer
"user.registration"
"email-notification"
"background_sync"
```

## Custom Activity Handlers

You can create custom activity handlers by implementing the `ActivityHandler` trait:

```rust
use runner_q::{ActivityContext, ActivityHandler, ActivityResult};
use async_trait::async_trait;
use serde_json::Value;

pub struct PaymentActivity {
    // Add your dependencies here (database connections, external APIs, etc.)
}

#[async_trait]
impl ActivityHandler for PaymentActivity {
    async fn handle(&self, payload: Value, context: ActivityContext) -> ActivityHandlerResult {
        // Parse the payment data using ? operator
        let amount = payload["amount"]
            .as_f64()
            .ok_or_else(|| ActivityError::NonRetry("Missing or invalid amount".to_string()))?;

        let currency = payload["currency"]
            .as_str()
            .unwrap_or("USD");

        println!("Processing payment: {} {}", amount, currency);

        // Validate amount
        if amount <= 0.0 {
            return Err(ActivityError::NonRetry("Invalid amount".to_string()));
        }

        // Simulate payment processing
        Ok(Some(serde_json::json!({
            "transaction_id": "txn_123456",
            "amount": amount,
            "currency": currency,
            "status": "completed"
        })))
    }

    fn activity_type(&self) -> String {
        "process_payment".to_string()
    }
}

// Register the handler
worker_engine.register_activity("process_payment".to_string(), Arc::new(PaymentActivity {}));
```

## Activity Priority and Options

Activities can be configured using the `ActivityOption` struct:

```rust
use runner_q::{ActivityPriority, ActivityOption};

// High priority with custom retry and timeout settings
let future = worker_engine.execute_activity(
    "send_email".to_string(),
    serde_json::json!({"to": "user@example.com"}),
    Some(ActivityOption {
        priority: Some(ActivityPriority::Critical), // Highest priority
        max_retries: 10,                            // Retry up to 10 times
        timeout_seconds: 900,                       // 15 minute timeout
    })
).await?;

// Use default options (Normal priority, 3 retries, 300s timeout)
let future = worker_engine.execute_activity(
    "send_email".to_string(),
    serde_json::json!({"to": "user@example.com"}),
    None
).await?;
```

Available priorities:
- `ActivityPriority::Critical` - Highest priority (processed first)
- `ActivityPriority::High` - High priority
- `ActivityPriority::Normal` - Default priority
- `ActivityPriority::Low` - Lowest priority

## Getting Activity Results

Activities can return results that can be retrieved asynchronously:

```rust
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct EmailResult {
    message: String,
    status: String,
}

let future = worker_engine.execute_activity(
    "send_email".to_string(),
    serde_json::json!({"to": "user@example.com"}),
    None
).await?;

// Get the result (this will wait until the activity completes)
let result_value = future.get_result().await?;
let email_result: EmailResult = serde_json::from_value(result_value)?;
println!("Email result: {:?}", email_result);
```

## Activity Orchestration

Activities can execute other activities using the `ActivityExecutor` available in the `ActivityContext`. This enables powerful workflow orchestration with the improved fluent API:

```rust
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityPriority, ActivityError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct OrderData {
    id: String,
    customer_email: String,
    items: Vec<String>,
}

pub struct OrderProcessingActivity;

#[async_trait]
impl ActivityHandler for OrderProcessingActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
        let order_id = payload["order_id"]
            .as_str()
            .ok_or_else(|| ActivityError::NonRetry("Missing order_id".to_string()))?;
        
        // Step 1: Validate payment using fluent API
        let _payment_future = context.activity_executor
            .activity("validate_payment")
            .payload(serde_json::json!({"order_id": order_id}))
            .priority(ActivityPriority::High)
            .max_retries(3)
            .timeout(std::time::Duration::from_secs(120))
            .execute()
            .await.map_err(|e| ActivityError::Retry(format!("Failed to enqueue payment validation: {}", e)))?;
        
        // Step 2: Update inventory
        let _inventory_future = context.activity_executor
            .activity("update_inventory")
            .payload(serde_json::json!({"order_id": order_id}))
            .execute()
            .await.map_err(|e| ActivityError::Retry(format!("Failed to enqueue inventory update: {}", e)))?;
        
        // Step 3: Schedule delivery notification for later
        context.activity_executor
            .activity("send_delivery_notification")
            .payload(serde_json::json!({"order_id": order_id, "customer_email": payload["customer_email"]}))
            .priority(ActivityPriority::Normal)
            .max_retries(5)
            .timeout(std::time::Duration::from_secs(300))
            .delay(std::time::Duration::from_secs(3600)) // 1 hour
            .execute()
            .await.map_err(|e| ActivityError::Retry(format!("Failed to schedule notification: {}", e)))?;
        
        Ok(Some(serde_json::json!({
            "order_id": order_id,
            "status": "processing",
            "steps_initiated": ["payment_validation", "inventory_update", "delivery_notification"]
        })))
    }

    fn activity_type(&self) -> String {
        "process_order".to_string()
    }
}
```

### Benefits of Activity Orchestration

- **Modularity**: Break complex workflows into smaller, reusable activities
- **Reliability**: Each sub-activity has its own retry logic and error handling
- **Monitoring**: Track progress of individual workflow steps
- **Scalability**: Sub-activities can be processed by different workers
- **Flexibility**: Different priority levels and timeouts for different steps
- **Scheduling**: Schedule activities for future execution
- **Fluent API**: Clean, readable activity execution with method chaining

## Metrics and Monitoring

Runner-Q provides comprehensive metrics collection through the `MetricsSink` trait, allowing you to integrate with your preferred monitoring system.

### Basic Metrics Implementation

```rust
use runner_q::{MetricsSink, WorkerEngine};
use std::time::Duration;
use std::sync::Arc;

// Simple logging metrics implementation
struct LoggingMetrics;

impl MetricsSink for LoggingMetrics {
    fn inc_counter(&self, name: &str, value: u64) {
        println!("METRIC: {} += {}", name, value);
    }
    
    fn observe_duration(&self, name: &str, duration: Duration) {
        println!("METRIC: {} = {:?}", name, duration);
    }
}

// Use with WorkerEngine
let engine = WorkerEngine::builder()
    .redis_url("redis://localhost:6379")
    .queue_name("my_app")
    .metrics(Arc::new(LoggingMetrics))
    .build()
    .await?;
```

### Prometheus Integration

```rust
use runner_q::{MetricsSink, WorkerEngine};
use std::time::Duration;
use std::sync::Arc;
use std::collections::HashMap;

// Prometheus metrics implementation
struct PrometheusMetrics {
    // Contains pre-registered Prometheus metrics
    counters: HashMap<String, prometheus::Counter>,
    histograms: HashMap<String, prometheus::Histogram>,
}

impl MetricsSink for PrometheusMetrics {
    fn inc_counter(&self, name: &str, value: u64) {
        if let Some(counter) = self.counters.get(name) {
            counter.inc_by(value as f64);
        }
    }
    
    fn observe_duration(&self, name: &str, duration: Duration) {
        if let Some(histogram) = self.histograms.get(name) {
            histogram.observe(duration.as_secs_f64());
        }
    }
}

// Custom metrics implementation
struct CustomMetrics {
    activity_completed: u64,
    activity_failed: u64,
    activity_retry: u64,
    total_execution_time: Duration,
}

impl MetricsSink for CustomMetrics {
    fn inc_counter(&self, name: &str, value: u64) {
        match name {
            "activity_completed" => {
                // Update your custom counter
                println!("Activities completed: {}", value);
            }
            "activity_failed_non_retry" => {
                // Track non-retryable failures
                println!("Activities failed (non-retry): {}", value);
            }
            "activity_retry" => {
                // Track retry attempts
                println!("Activity retries: {}", value);
            }
            _ => {}
        }
    }
    
    fn observe_duration(&self, name: &str, duration: Duration) {
        match name {
            "activity_execution" => {
                // Track execution times
                println!("Activity execution time: {:?}", duration);
            }
            _ => {}
        }
    }
}
```

### Available Metrics

The library automatically collects the following metrics:

- **`activity_completed`** - Number of activities completed successfully
- **`activity_retry`** - Number of activities that requested retry
- **`activity_failed_non_retry`** - Number of activities that failed permanently
- **`activity_timeout`** - Number of activities that timed out

### No-op Metrics

If you don't need metrics collection, you can use the built-in `NoopMetrics`:

```rust
use runner_q::{NoopMetrics, MetricsSink};
use std::time::Duration;

let metrics = NoopMetrics;

// These calls do nothing
metrics.inc_counter("activities_completed", 1);
metrics.observe_duration("activity_execution", Duration::from_secs(5));
```

## Advanced Features

### Activity Scheduling

Runner-Q supports scheduling activities for future execution with precise timestamp-based scheduling:

```rust
use runner_q::{WorkerEngine, ActivityPriority};
use serde_json::json;
use std::time::Duration;

// Get activity executor for scheduling
let activity_executor = engine.get_activity_executor();

// Schedule an activity to run in 1 hour
let future = activity_executor
    .activity("send_reminder")
    .payload(json!({"user_id": 123, "message": "Don't forget!"}))
    .delay(Duration::from_secs(3600)) // 1 hour from now
    .execute()
    .await?;

// Schedule for a specific time (using chrono)
use chrono::{DateTime, Utc, Duration as ChronoDuration};

let scheduled_time = Utc::now() + ChronoDuration::hours(2);
let future = activity_executor
    .activity("process_report")
    .payload(json!({"report_type": "monthly"}))
    .delay(Duration::from_secs(7200)) // 2 hours
    .execute()
    .await?;
```

### Redis Configuration

Fine-tune Redis connection behavior for your specific needs:

```rust
use runner_q::{WorkerEngine, RedisConfig};
use std::time::Duration;

let redis_config = RedisConfig {
    max_size: 100,                    // Maximum connections in pool
    min_idle: 10,                     // Minimum idle connections
    conn_timeout: Duration::from_secs(60),    // Connection timeout
    idle_timeout: Duration::from_secs(600),   // Idle connection timeout
    max_lifetime: Duration::from_secs(3600),  // Maximum connection lifetime
};

let engine = WorkerEngine::builder()
    .redis_url("redis://localhost:6379")
    .queue_name("my_app")
    .redis_config(redis_config)
    .build()
    .await?;
```

### Graceful Shutdown

The worker engine supports graceful shutdown with proper cleanup:

```rust
use runner_q::WorkerEngine;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// Start the engine in a background task
let engine = Arc::new(engine);
let engine_clone = engine.clone();

let engine_handle = tokio::spawn(async move {
    engine_clone.start().await
});

// Let it run for a while
sleep(Duration::from_secs(10)).await;

// Gracefully stop the engine
engine.stop().await;

// Wait for the engine to finish
engine_handle.await??;
```

### Activity Context and Metadata

Access rich context information in your activity handlers:

```rust
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult};
use async_trait::async_trait;

#[async_trait]
impl ActivityHandler for MyActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
        // Access activity metadata
        println!("Processing activity {} of type {}", context.activity_id, context.activity_type);
        println!("This is retry attempt #{}", context.retry_count);
        
        // Check for cancellation
        if context.cancel_token.is_cancelled() {
            return Err(ActivityError::NonRetry("Activity was cancelled".to_string()));
        }
        
        // Access custom metadata
        if let Some(correlation_id) = context.metadata.get("correlation_id") {
            println!("Correlation ID: {}", correlation_id);
        }
        
        Ok(Some(serde_json::json!({"status": "processed"})))
    }
    
    fn activity_type(&self) -> String {
        "my_activity".to_string()
    }
}
```

### Queue Statistics

Monitor queue performance and health using the inspector:

```rust
use runner_q::{WorkerEngine, QueueStats};

let engine = WorkerEngine::builder()
    .redis_url("redis://localhost:6379")
    .queue_name("my_app")
    .build()
    .await?;

// Get the inspector
let inspector = engine.inspector();

// Get queue statistics
let stats: QueueStats = inspector.stats().await?;

println!("Queue stats:");
println!("  Pending activities: {}", stats.pending_activities);
println!("  Processing activities: {}", stats.processing_activities);
println!("  Scheduled activities: {}", stats.scheduled_activities);
println!("  Dead letter queue size: {}", stats.dead_letter_activities);
println!("Priority distribution:");
println!("  Critical: {}", stats.critical_priority);
println!("  High: {}", stats.high_priority);
println!("  Normal: {}", stats.normal_priority);
println!("  Low: {}", stats.low_priority);
```

For a visual dashboard with real-time updates, see the [Observability Console](#observability-console) section.

## Error Handling

The library provides comprehensive error handling with clear separation between retryable and non-retryable errors.

### Activity Handler Results

In your activity handlers, you can use the convenient `ActivityHandlerResult` type with the `?` operator for clean error handling:

```rust
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
use async_trait::async_trait;
use serde_json::Value;

#[derive(serde::Deserialize)]
struct MyData {
    id: String,
    value: String,
}

#[async_trait]
impl ActivityHandler for MyActivity {
    async fn handle(&self, payload: Value, context: ActivityContext) -> ActivityHandlerResult {
        // Use ? operator for automatic error conversion
        let data: MyData = serde_json::from_value(payload)?;

        // Validate data
        if data.id.is_empty() {
            return Err(ActivityError::NonRetry("Invalid data format".to_string()));
        }

        // Perform operation that might temporarily fail
        let result = external_api_call(&data)
            .await
            .map_err(|e| ActivityError::Retry(format!("API call failed: {}", e)))?;

        // Return success with result data
        Ok(Some(serde_json::json!({"result": result})))
    }

    fn activity_type(&self) -> String {
        "my_activity".to_string()
    }
}
```

**Error Types:**
- `ActivityError::Retry(message)` - Will be retried with exponential backoff
- `ActivityError::NonRetry(message)` - Will not be retried
- Any error implementing `Into<ActivityError>` can be used with `?`

### Dead Letter Callback

When an activity exhausts all retries, it moves to the dead letter queue. You can handle this event by implementing the optional `on_dead_letter` callback:

```rust
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult};
use async_trait::async_trait;

#[async_trait]
impl ActivityHandler for MyActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
        // Activity logic
        Ok(None)
    }

    fn activity_type(&self) -> String {
        "my_activity".to_string()
    }

    async fn on_dead_letter(
        &self,
        payload: serde_json::Value,
        context: ActivityContext,
        error: String,
    ) {
        // Called once when activity enters dead letter state
        // Use for cleanup, notifications, or logging
        eprintln!("Activity {} dead-lettered: {}", context.activity_id, error);
    }
}
```

The callback has a default empty implementation, so existing handlers continue to work without modification.

### Worker Engine Errors

```rust
use runner_q::{WorkerEngine, WorkerError, ActivityPriority};
use serde_json::json;
use std::time::Duration;

// Using the fluent API for error handling
let activity_executor = engine.get_activity_executor();

match activity_executor
    .activity("my_activity")
    .payload(json!({"id": "123", "value": "test"}))
    .execute()
    .await 
{
    Ok(future) => {
        // Activity was successfully enqueued
        match future.get_result().await {
            Ok(result) => match result {
                Some(data) => println!("Activity completed: {:?}", data),
                None => println!("Activity completed with no result"),
            },
            Err(WorkerError::Timeout) => println!("Activity timed out"),
            Err(WorkerError::CustomError(msg)) => println!("Activity failed: {}", msg),
            Err(e) => println!("Activity failed: {}", e),
        }
    }
    Err(e) => println!("Failed to enqueue activity: {}", e),
}
```

### Error Recovery Patterns

```rust
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
use async_trait::async_trait;

#[async_trait]
impl ActivityHandler for ResilientActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
        // Check retry count for different strategies
        match context.retry_count {
            0..=2 => {
                // First few attempts: retry on any error
                self.process_with_retry(payload).await
            }
            3..=5 => {
                // Middle attempts: more conservative retry
                self.process_conservative(payload).await
            }
            _ => {
                // Final attempts: only retry on specific errors
                self.process_final_attempt(payload).await
            }
        }
    }

    fn activity_type(&self) -> String {
        "resilient_activity".to_string()
    }
}

impl ResilientActivity {
    async fn process_with_retry(&self, payload: serde_json::Value) -> ActivityHandlerResult {
        // Implementation that retries on any error
        Ok(Some(serde_json::json!({"status": "processed"})))
    }

    async fn process_conservative(&self, payload: serde_json::Value) -> ActivityHandlerResult {
        // More conservative processing
        Ok(Some(serde_json::json!({"status": "processed_conservative"})))
    }

    async fn process_final_attempt(&self, payload: serde_json::Value) -> ActivityHandlerResult {
        // Final attempt with minimal retry
        Ok(Some(serde_json::json!({"status": "processed_final"})))
    }
}
```

### Default Values

When using the builder pattern, sensible defaults are provided:

```rust
use runner_q::WorkerEngine;

// Uses these defaults:
// - redis_url: "redis://127.0.0.1:6379"
// - queue_name: "default"
// - max_workers: 10
// - schedule_poll_interval: 5 seconds
let engine = WorkerEngine::builder().build().await?;
```

### Redis Configuration

Fine-tune Redis connection behavior:

```rust
use runner_q::{RedisConfig, WorkerEngine};
use std::time::Duration;

let redis_config = RedisConfig {
    max_size: 100,                              // Maximum connections in pool
    min_idle: 10,                               // Minimum idle connections
    conn_timeout: Duration::from_secs(60),      // Connection timeout
    idle_timeout: Duration::from_secs(600),     // Idle connection timeout
    max_lifetime: Duration::from_secs(3600),    // Maximum connection lifetime
};

let engine = WorkerEngine::builder()
    .redis_config(redis_config)
    .build()
    .await?;
```

## Observability Console

Runner-Q includes a built-in web-based observability console for monitoring and managing your activity queues in real-time.

### Features

- **Real-time Updates** - Server-Sent Events (SSE) for instant activity updates
- **Live Statistics** - Monitor queue health with processing, pending, scheduled, and dead-letter counts
- **Priority Distribution** - See activity breakdown by priority level (Critical, High, Normal, Low)
- **Activity Management** - Browse and search activities across all queues (pending, processing, scheduled, completed, dead-letter)
- **Activity Results** - View execution results and outputs for completed activities
- **Event Timeline** - Detailed activity lifecycle events with multiple view modes
- **7-Day History** - Query completed activities for up to 7 days
- **Zero Setup** - No build tools, npm, or dependencies required

### Dashboard Preview

![RunnerQ Console Dashboard](asset/ui-dashbord.png)

### Quick Start

The console is designed to work just like Swagger UI - simply pass an inspector instance:

```rust
use runner_q::{runnerq_ui, WorkerEngine};
use axum::{serve, Router};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let engine = WorkerEngine::builder()
        .redis_url("redis://127.0.0.1:6379")
        .queue_name("my_app")
        .build()
        .await?;

    // Get inspector from engine (automatically enables event streaming)
    let inspector = engine.inspector();

    // Nest the console at /console - just like Swagger UI!
    let app = Router::new()
        .nest("/console", runnerq_ui(inspector));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await?;
    println!("✨ RunnerQ Console: http://localhost:8081/console");
    
    serve(listener, app).await?;
    Ok(())
}
```

### Integration with Existing Apps

You can easily integrate the console into your existing Axum application:

```rust
use runner_q::{runnerq_ui, WorkerEngine};
use axum::Router;

let engine = WorkerEngine::builder()
    .redis_url("redis://localhost:6379")
    .queue_name("my_app")
    .build()
    .await?;

let inspector = engine.inspector();

// Your existing app routes
let app = Router::new()
    .route("/api/users", get(list_users))
    .route("/api/posts", get(list_posts))
    // Add the console
    .nest("/console", runnerq_ui(inspector))
    .with_state(app_state);
```

### API-Only Mode

If you prefer to build a custom UI, you can serve just the API:

```rust
use runner_q::observability_api;

let app = Router::new()
    .nest("/api/observability", observability_api(inspector));
```

### Example

See the complete example in `examples/console_ui.rs`:

```bash
# Start Redis
redis-server

# Run the console example
cargo run --example console_ui

# Open http://localhost:8081/console
```

For more details, see the [UI README](ui/README.md).

## License

This project is licensed under the MIT License - see the LICENSE file for details.
