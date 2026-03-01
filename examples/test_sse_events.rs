use async_trait::async_trait;
use axum::{serve, Router};
use runner_q::{
    runnerq_ui, storage::PostgresBackend, ActivityContext, ActivityError, ActivityHandler,
    ActivityHandlerResult, WorkerEngine,
};
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};

/// Test activity that simulates work
struct TestActivity;

#[async_trait]
impl ActivityHandler for TestActivity {
    fn activity_type(&self) -> String {
        "test_activity".to_string()
    }

    async fn handle(
        &self,
        payload: serde_json::Value,
        ctx: ActivityContext,
    ) -> ActivityHandlerResult {
        println!("🔄 Processing test activity: {:?}", payload);
        tokio::time::sleep(Duration::from_secs(5)).await;
        if ctx.retry_count < 2 {
            return Err(ActivityError::Retry("Test activity failed".to_string()));
        }
        println!("✅ Completed test activity");
        Ok(Some(serde_json::json!({"status": "completed"})))
    }
}

/// Example to test SSE event emission
///
/// This example:
/// 1. Starts a worker engine with a test activity handler
/// 2. Serves the console UI with SSE
/// 3. Automatically enqueues test activities every 5 seconds
/// 4. You should see events in the browser console
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable logging to see what's happening
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:runnerq@localhost:5432/runnerq".to_string());

    let backend = PostgresBackend::new(&database_url, "test_sse").await?;

    // Build worker engine
    let mut engine = WorkerEngine::builder()
        .backend(Arc::new(backend))
        .max_workers(4)
        .build()
        .await?;

    // Register test activity handler
    engine.register_activity("test_activity".to_string(), Arc::new(TestActivity));

    // Get inspector for UI - event streaming is auto-enabled
    let inspector = engine.inspector();

    // Clone executor for background task
    let executor = engine.get_activity_executor();

    // Start worker engine in background
    let engine_clone = Arc::new(engine);
    let engine_handle = {
        let engine = engine_clone.clone();
        tokio::spawn(async move {
            println!("🚀 Worker engine starting...");
            if let Err(e) = engine.start().await {
                eprintln!("❌ Worker engine error: {}", e);
            }
        })
    };

    // Enqueue test activities periodically
    tokio::spawn(async move {
        println!("⏳ Waiting 3 seconds before first test activity...");
        tokio::time::sleep(Duration::from_secs(3)).await;

        let mut counter = 1;
        loop {
            println!("\n📤 Enqueueing test activity #{}", counter);
            match executor
                .activity("test_activity")
                .payload(serde_json::json!({
                    "test": true,
                    "counter": counter,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }))
                .max_retries(5)
                .idempotency_key(
                    uuid::Uuid::new_v4().to_string(),
                    runner_q::OnDuplicate::ReturnExisting,
                )
                .execute()
                .await
            {
                Ok(_) => println!("✓ Activity #{} enqueued successfully", counter),
                Err(e) => eprintln!("✗ Failed to enqueue activity: {}", e),
            }

            counter += 1;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    // Build UI app
    let app = Router::new().nest("/console", runnerq_ui(inspector)).layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await?;
    let bound_addr = listener.local_addr()?;

    println!("\n╔══════════════════════════════════════════════════╗");
    println!("║  🎯 SSE Test Server Running                      ║");
    println!("╠══════════════════════════════════════════════════╣");
    println!("║  Console UI:  http://{}/console          ║", bound_addr);
    println!(
        "║  SSE Stream:  http://{}/console/api/observability/stream ║",
        bound_addr
    );
    println!("╠══════════════════════════════════════════════════╣");
    println!("║  📡 Events you should see:                       ║");
    println!("║     1. Enqueued   - When activity added         ║");
    println!("║     2. Dequeued   - When worker picks it up     ║");
    println!("║     3. Started    - When processing begins      ║");
    println!("║     4. Completed  - When processing finishes    ║");
    println!("╠══════════════════════════════════════════════════╣");
    println!("║  🔍 Check browser DevTools console for events   ║");
    println!("║  📊 Activities auto-enqueue every 5 seconds     ║");
    println!("╚══════════════════════════════════════════════════╝\n");

    serve(listener, app).await?;

    // Cleanup
    engine_handle.abort();
    Ok(())
}
