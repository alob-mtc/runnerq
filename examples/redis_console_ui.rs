//! Example: RunnerQ with Redis backend and Console UI.
//!
//! Uses the `runner_q_redis` crate for storage and the same observability UI as the
//! Postgres example. Requires a running Redis (or Valkey) instance.
//!
//! Run with:
//!   cargo run --example redis_console_ui
//!
//! Optionally set REDIS_URL (default: redis://127.0.0.1:6379).

use async_trait::async_trait;
use axum::{serve, Router};
use runner_q::{
    runnerq_ui, ActivityContext, ActivityHandler, ActivityHandlerResult, WorkerEngine,
};
use runner_q_redis::RedisBackend;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let backend = RedisBackend::builder()
        .redis_url(&redis_url)
        .queue_name("my_app")
        .build()
        .await?;

    let mut engine = WorkerEngine::builder()
        .backend(Arc::new(backend))
        .queue_name("my_app")
        .max_workers(3)
        .build()
        .await?;

    engine.register_activity("test_activity".to_string(), Arc::new(TestActivity));

    let inspector = engine.inspector();
    let executor = engine.get_activity_executor();

    let engine_clone = Arc::new(engine);
    let engine_handle = {
        let engine = engine_clone.clone();
        tokio::spawn(async move {
            println!("🚀 Worker engine starting (Redis backend)...");
            if let Err(e) = engine.start().await {
                eprintln!("❌ Worker engine error: {}", e);
            }
        })
    };

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

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8083").await?;
    let bound_addr = listener.local_addr()?;

    println!("✨ RunnerQ Console (Redis): http://{}/console", bound_addr);
    println!("   Backend: Redis @ {}", redis_url);
    println!("   Real-time updates via SSE — Press Ctrl+C to stop");

    serve(listener, app).await?;
    engine_handle.abort();
    Ok(())
}
