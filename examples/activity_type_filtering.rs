//! Example: Worker-level activity type filtering
//!
//! Demonstrates how to run multiple worker engines on the same queue where each
//! engine only processes specific activity types. This enables workload isolation
//! without separate queues or binaries.
//!
//! ## Prerequisites
//!
//! Start a PostgreSQL instance:
//! ```bash
//! docker run -d --name runnerq-postgres \
//!     -e POSTGRES_PASSWORD=runnerq \
//!     -e POSTGRES_DB=runnerq \
//!     -p 5432:5432 \
//!     postgres:16
//! ```
//!
//! ## Running
//!
//! ```bash
//! export DATABASE_URL="postgres://postgres:runnerq@localhost:5432/runnerq"
//! cargo run --example activity_type_filtering --features postgres
//! ```

use async_trait::async_trait;
use runner_q::{
    storage::PostgresBackend, ActivityContext, ActivityHandler, ActivityHandlerResult, WorkerEngine,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

struct EmailHandler;

#[async_trait]
impl ActivityHandler for EmailHandler {
    fn activity_type(&self) -> String {
        "send_email".to_string()
    }

    async fn handle(
        &self,
        payload: serde_json::Value,
        _ctx: ActivityContext,
    ) -> ActivityHandlerResult {
        info!("[email-node] sending email: {}", payload);
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(Some(serde_json::json!({"sent": true})))
    }
}

struct SmsHandler;

#[async_trait]
impl ActivityHandler for SmsHandler {
    fn activity_type(&self) -> String {
        "send_sms".to_string()
    }

    async fn handle(
        &self,
        payload: serde_json::Value,
        _ctx: ActivityContext,
    ) -> ActivityHandlerResult {
        info!("[sms] sending SMS: {}", payload);
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(Some(serde_json::json!({"sent": true})))
    }
}

struct TradeHandler;

#[async_trait]
impl ActivityHandler for TradeHandler {
    fn activity_type(&self) -> String {
        "execute_trade".to_string()
    }

    async fn handle(
        &self,
        payload: serde_json::Value,
        _ctx: ActivityContext,
    ) -> ActivityHandlerResult {
        info!("[trade-node] executing trade: {}", payload);
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(Some(serde_json::json!({"executed": true})))
    }
}

struct ReportHandler;

#[async_trait]
impl ActivityHandler for ReportHandler {
    fn activity_type(&self) -> String {
        "generate_report".to_string()
    }

    async fn handle(
        &self,
        payload: serde_json::Value,
        _ctx: ActivityContext,
    ) -> ActivityHandlerResult {
        info!("[catch-all] generating report: {}", payload);
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(Some(serde_json::json!({"generated": true})))
    }
}

/// Build a worker engine with the given activity_types filter.
/// Registers only the handlers that match the filter (or all if None).
async fn build_engine(
    backend: Arc<PostgresBackend>,
    activity_types: Option<&[&str]>,
    label: &str,
) -> WorkerEngine {
    let mut builder = WorkerEngine::builder()
        .backend(backend)
        .queue_name("filtering_example")
        .max_workers(2);

    if let Some(types) = activity_types {
        builder = builder.activity_types(types);
    }

    let mut engine = builder.build().await.expect("failed to build engine");

    let all_handlers: Vec<Arc<dyn ActivityHandler>> = vec![
        Arc::new(EmailHandler),
        Arc::new(SmsHandler),
        Arc::new(TradeHandler),
        Arc::new(ReportHandler),
    ];

    match activity_types {
        Some(types) => {
            for handler in &all_handlers {
                if types.contains(&handler.activity_type().as_str()) {
                    engine.register_activity(handler.activity_type(), handler.clone());
                }
            }
        }
        None => {
            for handler in &all_handlers {
                engine.register_activity(handler.activity_type(), handler.clone());
            }
        }
    }

    info!("Engine [{}] ready (filter: {:?})", label, activity_types);
    engine
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:runnerq@localhost:5432/runnerq".to_string());

    let backend = Arc::new(PostgresBackend::new(&database_url, "filtering_example").await?);
    info!("PostgreSQL backend initialised");

    // ── Node 1: emails only ────────────────────────────────────────────
    let email_engine = build_engine(
        backend.clone(),
        Some(&["send_email", "send_sms"]),
        "email-node",
    )
    .await;

    // ── Node 2: trades only ────────────────────────────────────────────
    let trade_engine = build_engine(
        backend.clone(),
        Some(&["execute_trade"]),
        "trade-node",
    )
    .await;

    // ── Node 3: catch-all (no filter) ──────────────────────────────────
    let catchall_engine = build_engine(backend.clone(), None, "catch-all").await;

    // Grab an executor from any engine to enqueue activities
    let executor = catchall_engine.get_activity_executor();

    // Start all three engines
    let email_engine = Arc::new(email_engine);
    let trade_engine = Arc::new(trade_engine);
    let catchall_engine = Arc::new(catchall_engine);

    let h1 = tokio::spawn({ let e = email_engine.clone();    async move { e.start().await } });
    let h2 = tokio::spawn({ let e = trade_engine.clone();    async move { e.start().await } });
    let h3 = tokio::spawn({ let e = catchall_engine.clone(); async move { e.start().await } });

    // Enqueue a mix of activity types
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let types = ["send_email", "send_sms", "execute_trade", "generate_report"];
        for (i, activity_type) in types.iter().cycle().take(12).enumerate() {
            info!("Enqueueing {} #{}", activity_type, i + 1);
            executor
                .activity(activity_type)
                .payload(serde_json::json!({ "seq": i + 1 }))
                .execute()
                .await
                .expect("enqueue failed");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        info!("All activities enqueued. Watch the logs to see each engine claim only its types.");
        tokio::time::sleep(Duration::from_secs(10)).await;
        std::process::exit(0);
    });

    println!("\n=== Activity Type Filtering Example ===");
    println!("  email-node  : send_email, send_sms");
    println!("  trade-node  : execute_trade");
    println!("  catch-all   : everything (including generate_report)");
    println!("  Press Ctrl+C to stop\n");

    tokio::select! {
        r = h1 => { r??; }
        r = h2 => { r??; }
        r = h3 => { r??; }
    }

    Ok(())
}
