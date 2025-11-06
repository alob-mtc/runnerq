# Debugging SSE Events Not Emitting

If you're seeing SSE connections succeed (200 OK) but not receiving events, follow these debugging steps:

## Quick Checklist

1. ✅ SSE endpoint returns 200 (not 503) - Connection established
2. ❓ Events are being generated - Activities being processed
3. ❓ Event channel is shared - Queue and Inspector use same channel
4. ❓ Workers are running - Engine is processing activities

## Common Issues

### Issue 1: Inspector Not From Engine

**Problem:** Creating inspector directly instead of from engine.

❌ **Wrong:**
```rust
let pool = runner_q::runner::redis::create_redis_pool(&config.redis_url).await?;
let inspector = QueueInspector::new(pool, queue_name.clone());
let runner_console = runnerq_ui(inspector);
```

✅ **Correct:**
```rust
// Use the existing worker_engine
let inspector = worker_engine.inspector();
let runner_console = runnerq_ui(inspector);
```

**Why:** The inspector from `worker_engine.inspector()` shares the same event channel that the queue uses to broadcast events.

### Issue 2: No Activities Being Processed

**Problem:** No activities are being enqueued or processed, so no events are generated.

**Test:** Manually enqueue an activity:

```rust
let executor = worker_engine.get_activity_executor();

executor
    .activity("test_activity")
    .payload(serde_json::json!({"test": true}))
    .execute()
    .await?;
```

**Expected:** You should see an `Enqueued` event in the browser console.

**Check browser console:**
```javascript
// Should see events like:
{
  "activity_id": "...",
  "timestamp": "...",
  "event_type": "Enqueued",
  "worker_id": null,
  "detail": {...}
}
```

### Issue 3: Worker Engine Not Started

**Problem:** Worker engine exists but isn't running, so activities stay in pending state.

**Check:** Ensure the worker engine is started:

```rust
// Start in background
tokio::spawn(async move {
    if let Err(e) = worker_engine.start().await {
        eprintln!("Worker engine error: {}", e);
    }
});
```

Or if you're using it in your AppState, make sure it's being started somewhere.

### Issue 4: Event Channel Not Shared

**Problem:** Queue and inspector have different event channels.

**Verify:** Add logging to check if events are being sent:

```rust
// In your code, before getting inspector
let inspector = worker_engine.inspector();

// Check if inspector has events enabled
if inspector.subscribe_events().is_some() {
    println!("✓ Inspector has event stream enabled");
} else {
    println!("✗ Inspector does NOT have event stream");
}
```

## Debugging Steps

### Step 1: Verify Connection

Open browser DevTools → Network tab:
- Look for `/console/api/observability/stream`
- Status should be `200` (pending/streaming)
- Type should be `eventsource` or `EventStream`

### Step 2: Test Event Generation

In your application code or via redis-cli, trigger an activity:

```bash
# Via your application API if you have one
curl -X POST http://localhost:8080/api/enqueue-activity

# Or manually via redis-cli
redis-cli ZADD my_app:priority_queue 2000000 "$(uuidgen):{\"activity_type\":\"test\",\"payload\":{}}"
```

### Step 3: Check Browser Console

Open browser console and look for SSE messages:

```javascript
// You should see logs like:
"Connected to real-time updates"
"Activity event received: {activity_id: '...', event_type: 'Enqueued', ...}"
```

If you see connection but no events, the problem is event generation, not SSE.

### Step 4: Add Debug Logging

Temporarily add logging to your Rust code:

```rust
// In src/queue/queue.rs, in record_event method after line 436:
let _ = tx.send(activity_event.clone());
println!("📡 Broadcast event: {:?}", activity_event);
```

Recompile and run. You should see console output when activities are processed.

### Step 5: Verify Worker Engine Setup

Ensure your worker engine is properly configured:

```rust
let worker_engine = WorkerEngine::builder()
    .redis_url(&config.redis_url)
    .queue_name(&config.worker.queue_name)
    .max_workers(10)  // Must be > 0
    .build()
    .await?;

// Register at least one activity handler
worker_engine.register_activity("test_activity", Arc::new(TestHandler));

// Get inspector AFTER engine is fully set up
let inspector = worker_engine.inspector();

// Start the engine (crucial!)
tokio::spawn(async move {
    worker_engine.start().await
});
```

## Expected Event Flow

Here's what should happen when an activity is processed:

1. **Enqueue** → `Enqueued` event
2. **Dequeue** → `Dequeued` event  
3. **Start** → `Started` event
4. **Complete** → `Completed` event (or `Failed`/`Retrying`)

Each should trigger an SSE message to the browser.

## Verification Script

Add this to your code to test the full flow:

```rust
use runner_q::ActivityHandler;
use async_trait::async_trait;

struct TestActivity;

#[async_trait]
impl ActivityHandler for TestActivity {
    async fn handle(&self, payload: serde_json::Value, _ctx: ActivityContext) -> ActivityHandlerResult {
        println!("Processing test activity: {:?}", payload);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(Some(serde_json::json!({"status": "done"})))
    }
    
    fn activity_type(&self) -> String {
        "test_activity".to_string()
    }
}

// In your setup:
worker_engine.register_activity("test_activity", Arc::new(TestActivity));

// After server starts, trigger a test:
let executor = worker_engine.get_activity_executor();
executor.activity("test_activity")
    .payload(serde_json::json!({"test": "data"}))
    .execute()
    .await?;
```

**Expected:** You should see 4 SSE events (Enqueued, Dequeued, Started, Completed) within ~1 second.

## Still Not Working?

If events still aren't showing up:

1. **Check Redis connection:**
   ```bash
   redis-cli PING
   # Should return: PONG
   ```

2. **Check queue keys:**
   ```bash
   redis-cli KEYS "*priority_queue*"
   redis-cli KEYS "activity:*"
   ```

3. **Check activity in queue:**
   ```bash
   redis-cli ZRANGE my_app:priority_queue 0 -1
   ```

4. **Enable tracing:**
   ```rust
   tracing_subscriber::fmt()
       .with_max_level(tracing::Level::DEBUG)
       .init();
   ```

5. **Check if inspector and queue share the same Redis pool:**
   - If creating separate connections, they won't share the event channel
   - Always use `worker_engine.inspector()`

## Advanced: Manual Event Channel Setup

If you really need to create an inspector separately (not recommended):

```rust
// Create the event channel FIRST
let (event_tx, _event_rx) = tokio::sync::broadcast::channel(64);

// Create the engine
let mut engine = WorkerEngine::builder()
    .redis_url(&redis_url)
    .queue_name(&queue_name)
    .build()
    .await?;

// Enable event stream with the channel
engine.enable_event_stream(64);

// Get inspector (now shares the channel)
let inspector = engine.inspector();

// Use in UI
let console = runnerq_ui(inspector);
```

But again, simply using `engine.inspector()` handles all of this automatically.

