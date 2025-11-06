use axum::{serve, Router};
use runner_q::{runnerq_ui, WorkerEngine};
use tower_http::cors::{Any, CorsLayer};

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

    let engine = WorkerEngine::builder()
        .redis_url(&redis_url)
        .queue_name("my_app")
        .max_workers(3)
        .build()
        .await?;

    // Get inspector from engine - event streaming is auto-enabled
    let inspector = engine.inspector();

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
    Ok(())
}
