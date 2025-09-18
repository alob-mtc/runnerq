# Runner-Q

A robust, scalable Redis-based activity queue and worker system for Rust applications.

## Features

- **Priority-based activity processing** - Support for Critical, High, Normal, and Low priority levels
- **Activity scheduling and retries** - Built-in retry mechanism with exponential backoff
- **Dead letter queue** - Failed activities are moved to a dead letter queue for inspection
- **Concurrent activity processing** - Configurable number of concurrent workers
- **Graceful shutdown** - Proper shutdown handling with signal support
- **Activity timeouts** - Configurable timeout per activity type
- **Activity metadata** - Support for custom metadata on activities
- **Redis persistence** - Activities are stored in Redis for durability

## Installation

```sh
cargo add runner_q
```

## Quick Start

```rust
use runner_q::{ActivityQueue, WorkerEngine, ActivityPriority, ActivityOption};
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
use runner_q::config::WorkerConfig;
use std::sync::Arc;
use async_trait::async_trait;

// Implement activity handler
pub struct SendEmailActivity;

#[async_trait]
impl ActivityHandler for SendEmailActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
        // Process the email activity - use ? operator for clean error handling
        let to = payload["to"]
            .as_str()
            .ok_or_else(|| ActivityError::NonRetry("Missing 'to' field".to_string()))?;

        println!("Sending email to: {}", to);

        Ok(Some(serde_json::json!({
            "message": format!("Email sent to {}", to),
            "status": "delivered"
        })))
    }

    fn activity_type(&self) -> String {
        "send_email".to_string()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create Redis connection pool
    let redis_pool = bb8_redis::bb8::Pool::builder()
        .build(bb8_redis::RedisConnectionManager::new("redis://127.0.0.1:6379")?)
        .await?;

    // Create worker engine
    let config = WorkerConfig::default();
    let mut worker_engine = WorkerEngine::new(redis_pool, config);

    // Register activity handler
    let send_email_activity = SendEmailActivity;
    worker_engine.register_activity("send_email".to_string(), Arc::new(send_email_activity));

    // Execute an activity with custom options
    let future = worker_engine.execute_activity(
        "send_email".to_string(),
        serde_json::json!({"to": "user@example.com", "subject": "Welcome!"}),
        Some(ActivityOption {
            priority: Some(ActivityPriority::High),
            max_retries: 5,
            timeout_seconds: 600, // 10 minutes
        })
    ).await?;

    // Execute an activity with default options
    let future2 = worker_engine.execute_activity(
        "send_email".to_string(),
        serde_json::json!({"to": "admin@example.com"}),
        None // Uses default priority (Normal), 3 retries, 300s timeout
    ).await?;

    // Spawn a task to handle the result
    tokio::spawn(async move {
        if let Ok(result) = future.get_result().await {
            match result {
                Some(data) => {
                    println!("Email result: {:?}", data);
                }
                _ => {}
            }
        }
    });

    // Start the worker engine (this will run indefinitely)
    worker_engine.start().await?;

    Ok(())
}
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

## Configuration

```rust
use runner_q::config::WorkerConfig;

let config = WorkerConfig {
    queue_name: "my_queue".to_string(),
    max_concurrent_activities: 10,
    redis_url: "redis://127.0.0.1:6379".to_string(),
};
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

## Nested Activities (Activity Orchestration)

Activities can execute other activities using the `ActivityExecutor` available in the `ActivityContext`. This enables powerful workflow orchestration:

```rust
use runner_q::{ActivityExecutor, ActivityOption, ActivityHandlerResult, ActivityError, ActivityPriority};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct OrderData {
    id: String,
    customer_email: String,
    items: Vec<String>,
}

#[async_trait]
impl ActivityHandler for ProcessOrderActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
        // Parse order data using ? operator for clean error handling
        let order: OrderData = serde_json::from_value(payload)?;

        // Execute sub-activities using the context's worker engine

        // 1. Update inventory
        let _inventory_future = context.worker_engine.execute_activity(
            "update_inventory".to_string(),
            serde_json::json!({"item": "product_123", "quantity": -1}),
            Some(ActivityOption {
                priority: Some(ActivityPriority::High),
                max_retries: 3,
                timeout_seconds: 60,
                scheduled_at: None,
            })
        ).await.map_err(|e| ActivityError::Retry(format!("Failed to update inventory: {}", e)))?;

        // 2. Send confirmation email
        let _email_future = context.worker_engine.execute_activity(
            "send_email".to_string(),
            serde_json::json!({"to": order.customer_email, "template": "order_confirmation"}),
            None
        ).await.map_err(|e| ActivityError::Retry(format!("Failed to send email: {}", e)))?;

        // 3. Log the transaction
        context.worker_engine.execute_activity(
            "log_transaction".to_string(),
            serde_json::json!({"order_id": order.id, "status": "processed"}),
            None
        ).await.map_err(|e| ActivityError::NonRetry(format!("Failed to log transaction: {}", e)))?;

        // Return success with result data
        Ok(Some(serde_json::json!({
            "order_id": order.id,
            "status": "completed"
        })))
    }

    fn activity_type(&self) -> String {
        "process_order".to_string()
    }
}
```

### Benefits of Nested Activities

- **Modularity**: Break complex workflows into smaller, reusable activities
- **Reliability**: Each sub-activity has its own retry logic and error handling
- **Monitoring**: Track progress of individual workflow steps
- **Scalability**: Sub-activities can be processed by different workers
- **Flexibility**: Different priority levels and timeouts for different steps

## Error Handling

The library provides comprehensive error handling:

### Activity Handler Results

In your activity handlers, you can use the convenient `ActivityHandlerResult` type with the `?` operator for clean error handling:

```rust
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};

#[async_trait]
impl ActivityHandler for MyActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {
        // Use ? operator for automatic error conversion
        let data: MyData = serde_json::from_value(payload)?;

        // Validate data
        if data.is_invalid() {
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
- `ActivityError::NonRetry(message)` - Will not be retried, goes to dead letter queue
- Any error implementing `Into<ActivityError>` can be used with `?`

### Worker Engine Errors

```rust
use runner_q::WorkerError;

match worker_engine.execute_activity(activity_type.to_string(), payload, options).await {
    Ok(future) => {
        // Activity was successfully enqueued
        match future.get_result().await {
            Ok(result) => match result {
                Some(data) => println!("Activity completed: {:?}", result),
                None => {}
            },
            Err(WorkerError::Timeout) => println!("Activity timed out"),
            Err(e) => println!("Activity failed: {}", e),
        }
    }
    Err(e) => println!("Failed to enqueue activity: {}", e),
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
