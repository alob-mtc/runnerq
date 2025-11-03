use runner_q::{ActivityQueue, WorkerEngine, ActivityPriority};
use runner_q::{ActivityHandler, ActivityContext, ActivityHandlerResult, ActivityError};
use runner_q::config::WorkerConfig;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use tracing::info;
use std::time::Duration;


// Implement activity handler
pub struct PingActivity;

#[async_trait]
impl ActivityHandler for PingActivity {
    async fn handle(&self, payload: serde_json::Value, context: ActivityContext) -> ActivityHandlerResult {        
        let input:PingInput = serde_json::from_value(payload)
            .map_err(|_| ActivityError::NonRetry("Invalid payload format".to_string()))?;

        Ok(Some(
            serde_json::to_value(Pong{operation_id: input.operation_id})
                .map_err(|_| ActivityError::NonRetry("Invalid pong format".to_string()))?
        ))
    }

    fn activity_type(&self) -> String {
        "ping".to_string()
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct PingInput {
    operation_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pong {
    operation_id: String,
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();
 let mut engine = WorkerEngine::builder()
     .redis_url("redis://localhost:6379")
     .queue_name("my_app")
     .max_workers(10)
     .schedule_poll_interval(Duration::from_secs(30))
     .build()
     .await?;


    // Register activity handler
    engine.register_activity("ping".to_string(), Arc::new(PingActivity));

    let future = engine
        .get_activity_executor()
        .activity("ping")
        .payload(serde_json::to_value(Pong{operation_id: "0x1".into()}).unwrap(),)
        .priority(ActivityPriority::High)
        .max_retries(5)
        .timeout(Duration::from_secs(600))
        .delay(Duration::from_secs(60))
        .execute()
        .await?;

    // Spawn a task to handle the result
    tokio::spawn(async move {
        if let Ok(result) = future.get_result().await {
            match result {
                None => {}
                Some(data) => {
                    let pong: Pong = serde_json::from_value(data).unwrap();
                    info!("pong::{:?}", pong.operation_id);
                }
            }
        }
    });

    // Start the worker engine (this will run indefinitely)
    engine.start().await?;

    Ok(())
}
