use async_trait::async_trait;
use axum::{serve, Router};
use runner_q::{runnerq_ui, ActivityContext, ActivityHandler, ActivityHandlerResult, WorkerEngine};
use std::{sync::Arc, time::Duration};
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
        _ctx: ActivityContext,
    ) -> ActivityHandlerResult {
        println!("🔄 Processing test activity: {:?}", payload);
        tokio::time::sleep(Duration::from_secs(5)).await;
        println!("✅ Completed test activity");
        Ok(Some(serde_json::json!({"status": "completed"})))
    }
}

/// Example showing how to serve the RunnerQ Console UI with real-time updates
///
/// This example demonstrates the simplest way to add observability to your RunnerQ instance.
/// The UI will be available at http://localhost:8081/console with real-time SSE updates.
///
/// Event streaming is automatically enabled internally - no configuration required!
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let mut engine = WorkerEngine::builder()
        .redis_url(&redis_url)
        .queue_name("my_app")
        .max_workers(3)
        .build()
        .await?;

    // Register test activity handler
    engine.register_activity("test_activity".to_string(), Arc::new(TestActivity));

    // Get inspector from engine - event streaming is auto-enabled
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

    let app = Router::new().nest("/console", runnerq_ui(inspector)).layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await?;
    let bound_addr = listener.local_addr()?;

    println!("✨ RunnerQ Console: http://{}/console", bound_addr);
    println!("   Real-time updates enabled via SSE");
    println!("   Press Ctrl+C to stop");

    serve(listener, app).await?;
    // Cleanup
    engine_handle.abort();
    Ok(())
}
