# RunnerQ Console UI

This directory contains the observability UI for RunnerQ.

## Single-Page HTML Version

The `runnerq-console.html` is a self-contained, single-page HTML file that provides a complete observability dashboard for RunnerQ. It's designed to be embedded in your Rust application, similar to how Swagger UI works.

### Features

- **Real-time Updates**: Server-Sent Events (SSE) for instant activity updates
- **Live Statistics**: Queue stats with processing, pending, scheduled, and dead-letter counts
- **Priority Distribution**: Real-time breakdown of activities by priority level
- **Activity Management**: Browse pending, processing, scheduled, completed, and dead-letter activities
- **Activity Results**: View execution results and outputs for completed activities
- **Event Timeline**: Detailed activity lifecycle events with multiple view modes
- **Search & Filter**: Filter activities by type, status, and ID
- **Modern UI**: Temporal-inspired design with dark theme and responsive layout
- **Zero Build Step**: No npm, webpack, or build process required
- **Self-Contained**: All HTML, CSS, and JavaScript in a single file
- **7-Day History**: Completed activities queryable for 7 days

### Usage

Simply use the provided Rust helper functions to serve the UI:

```rust
use runner_q::{runnerq_ui, storage::PostgresBackend, WorkerEngine};
use axum::{serve, Router};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let backend = PostgresBackend::new("postgres://localhost/mydb", "my_app").await?;
    let engine = WorkerEngine::builder()
        .backend(Arc::new(backend))
        .queue_name("my_app")
        .build()
        .await?;
    let inspector = engine.inspector();

    let app = Router::new()
        .nest("/console", runnerq_ui(inspector));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await?;
    println!("✨ RunnerQ Console: http://localhost:8081/console");
    serve(listener, app).await?;
    Ok(())
}
```

That's it! The UI will be available at `http://localhost:8081/console` with the API automatically mounted at `http://localhost:8081/console/api/observability`.

### API-Only Mode

If you want to serve just the API (e.g., for a custom UI):

```rust
use runner_q::{observability_api, QueueInspector};

let app = Router::new()
    .nest("/api/observability", observability_api(inspector));
```

### API Endpoints

The UI expects these endpoints to be available:

- `GET /api/observability/stats` - Queue statistics (including priority distribution)
- `GET /api/observability/stream` - Server-Sent Events stream for real-time updates
- `GET /api/observability/activities/pending` - List pending activities
- `GET /api/observability/activities/processing` - List processing activities
- `GET /api/observability/activities/scheduled` - List scheduled activities
- `GET /api/observability/activities/completed` - List completed activities (7-day history)
- `GET /api/observability/activities/dead_letter` - List dead-letter activities
- `GET /api/observability/activities/{id}` - Get activity details
- `GET /api/observability/activities/{id}/events` - Get activity events
- `GET /api/observability/activities/{id}/result` - Get activity execution result
- `GET /api/observability/dead-letter` - Get dead-letter records

### Customization

The UI **automatically detects** the correct API path based on where it's served from:
- Served at `/console` → API at `/console/api/observability`
- Served at `/ui` → API at `/ui/api/observability`
- Served at `/` → API at `/api/observability`

To override the auto-detection and use a custom API base URL, set `window.RUNNERQ_API_BASE` before the page loads:

```html
<script>
    window.RUNNERQ_API_BASE = '/custom/api/path';
</script>
```

### Real-time Updates

The UI uses Server-Sent Events (SSE) for instant real-time updates with zero configuration.

#### How it Works

1. When you call `runnerq_ui(inspector)` or `engine.inspector()`, event streaming is automatically enabled
2. The UI connects via SSE to `/api/observability/stream`
3. Every activity lifecycle event triggers instant UI updates
4. Connection automatically reconnects if interrupted
5. Works in modern browsers only (no polling fallback)

#### Event Types

The UI receives real-time events for:
- `Enqueued` - Activity added to queue
- `Started` - Worker begins processing
- `Completed` - Activity finished successfully
- `Failed` - Activity failed (will retry)
- `DeadLetter` - Activity moved to dead-letter queue
- `Heartbeat` - Worker is still processing

#### Usage

Get an inspector from the engine (built with any backend that implements `InspectionStorage`, e.g. Postgres or Redis via `runner_q_redis`):

```rust
let backend = PostgresBackend::new("postgres://localhost/mydb", "my_app").await?;
let engine = WorkerEngine::builder()
    .backend(Arc::new(backend))
    .queue_name("my_app")
    .build()
    .await?;
let inspector = engine.inspector();
let app = Router::new().nest("/console", runnerq_ui(inspector));
```

## Running the Example

```bash
# With PostgreSQL: set DATABASE_URL and run the console example
DATABASE_URL=postgres://localhost/mydb cargo run --example console_ui

# With Redis: set REDIS_URL and run the Redis example
REDIS_URL=redis://127.0.0.1:6379 cargo run --example redis_console_ui

# Open http://localhost:8081/console (or :8082 for redis_console_ui)
```

## Development

The console is a single HTML file (`runnerq-console.html`) with embedded CSS and JavaScript. To modify:

1. Edit `ui/runnerq-console.html` directly
2. Test by running `cargo run --example console_ui`
3. No build step required - changes are immediately visible on page refresh

The UI is designed to work in modern browsers and uses:
- Vanilla JavaScript (no framework)
- CSS Grid and Flexbox for layout
- Server-Sent Events for real-time updates
- Fetch API for HTTP requests

