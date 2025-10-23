use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub queue_name: String,
    pub max_concurrent_activities: usize,
    pub redis_url: String,
    pub schedule_poll_interval_seconds: Option<u64>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            max_concurrent_activities: 10,
            redis_url: "redis://127.0.0.1:6379".to_string(),
            schedule_poll_interval_seconds: None,
        }
    }
}
