# Testing SSE Real-time Updates

This document describes how to test the new SSE (Server-Sent Events) real-time updates functionality.

## Prerequisites

1. Redis server running: `redis-server`
2. Rust toolchain installed
3. Project with Cargo.toml configured

## Test Steps

### 1. Compile the Example

```bash
cargo check --example console_ui
cargo build --example console_ui
```

Expected: No compilation errors.

### 2. Run the Console UI Example

```bash
# Make sure Redis is running first
redis-server

# In another terminal, run the example
cargo run --example console_ui
```

Expected output:
```
✨ RunnerQ Console: http://127.0.0.1:8081/console
   Real-time updates enabled via SSE
   Press Ctrl+C to stop
```

### 3. Test SSE Connection

1. Open browser to http://localhost:8081/console
2. Open Browser DevTools (F12)
3. Go to Network tab
4. Look for a request to `/console/api/observability/stream`
5. Check that it shows:
   - Type: `eventsource` or `EventStream`
   - Status: `200 OK` (pending/streaming)

Expected: SSE connection established and maintained.

### 4. Test Real-time Updates

#### In another terminal, enqueue some activities:

```bash
# If you have the activity example
cargo run --example activity

# Or use redis-cli to manually trigger events
redis-cli ZADD my_app:priority_queue 2000000 "test-activity-id:{...}"
```

Expected behavior:
- Stats update instantly (no delay)
- Activity appears in the appropriate collection tab immediately
- No polling requests in Network tab (only SSE stream)

### 5. Test Reconnection

1. With the console UI open and connected
2. Stop the server (Ctrl+C)
3. Check the UI shows "Lost connection to server. Reconnecting..."
4. Restart the server
5. Wait 3 seconds

Expected:
- Error banner shows during disconnect
- UI automatically reconnects after 3 seconds
- SSE stream resumes in Network tab
- Updates continue working

### 6. Test Event Types

Test that different event types trigger appropriate updates:

| Event Type | Expected Behavior |
|------------|------------------|
| `Enqueued` | Stats update, appears in Pending tab |
| `Started` | Moves to Processing tab |
| `Completed` | Removes from Processing tab, stats update |
| `Failed` | Moves to Dead Letter tab |
| `Retrying` | Moves to Scheduled tab |
| `Scheduled` | Appears in Scheduled tab |

### 7. Test Multiple Browser Tabs

1. Open console in 2-3 browser tabs
2. Trigger an activity event
3. Verify all tabs update simultaneously

Expected: All tabs receive SSE events and update in real-time.

### 8. Verify No Polling

1. With console open, filter Network tab to XHR/Fetch
2. After initial load, should only see SSE stream
3. No repeated GET requests to `/stats` or `/activities/*`

Expected: Zero polling requests, only the SSE EventStream.

## Debugging

### SSE Not Connecting

If SSE connection fails (404 or 503):

1. Check inspector has event stream:
   ```rust
   let inspector = engine.inspector();
   // Event stream is auto-enabled by inspector() method
   ```

2. Verify route is registered:
   ```rust
   // In src/observability/ui.rs
   .route("/api/observability/stream", get(event_stream))
   ```

3. Check browser console for errors

### Events Not Received

If connected but not receiving events:

1. Verify activities are being processed
2. Check Redis for activity keys: `redis-cli KEYS "activity:*"`
3. Look at server logs for event broadcast errors
4. Verify event types match in `getCollectionForEvent()`

### Browser Compatibility

SSE is supported in all modern browsers. If issues occur:

- Chrome/Edge: Full support
- Firefox: Full support  
- Safari: Full support
- IE: Not supported (use modern browser)

## Performance Testing

### Load Test

To test with high throughput:

```bash
# Enqueue many activities rapidly
for i in {1..100}; do
  # Your activity enqueue command
done
```

Expected:
- UI remains responsive
- SSE stream handles all events
- No lagging or dropped updates (check browser console)

### Memory Test

Monitor browser memory with many events:

1. Open DevTools → Memory
2. Take heap snapshot
3. Generate 1000+ events
4. Take another snapshot
5. Compare memory usage

Expected: Reasonable memory growth, no major leaks.

## Success Criteria

✅ SSE connection establishes on page load  
✅ Real-time updates arrive instantly (<100ms)  
✅ Auto-reconnection works after disconnect  
✅ Multiple tabs work independently  
✅ No polling requests occur  
✅ All event types handled correctly  
✅ UI remains responsive under load  
✅ No console errors  

## Troubleshooting

### Cargo.toml Not Found

If you see "could not find Cargo.toml", ensure you're in the correct directory with a proper Rust project structure including Cargo.toml at the root.

### Redis Connection Error

```
Error: Failed to connect to Redis
```

Solution: Start Redis server first: `redis-server`

### Port Already in Use

```
Error: Address already in use (os error 48)
```

Solution: Change port in example or kill process using port 8081.

