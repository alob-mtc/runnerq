
use crate::worker::WorkerError;
use crate::{activity::activity::{Activity,ActivityStatus, ActivityOption}, ActivityPriority};
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use crate::runner::redis::{create_redis_pool};
use testcontainers::{core::{IntoContainerPort, WaitFor, ContainerAsync}, runners::AsyncRunner, GenericImage, ImageExt};
// use crate::queue::{ActivityOption};


pub type TestResult = Result<(), Box<dyn std::error::Error + 'static>>;
    
pub async fn setup_redis_test_environment() -> (Pool<RedisConnectionManager>, ContainerAsync<GenericImage>)  {
   let container = GenericImage::new("redis", "alpine3.22")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_network("bridge")
        .with_env_var("DEBUG", "1")
        .start()
        .await
        .expect("Failed to start Redis");
    
    let host = container.get_host().await.expect("Failed to get host");
    let host_port = container.get_host_port_ipv4(6379).await.expect("Failed to get host port");
    let pool = create_redis_pool(&format!("redis://{host}:{host_port}")).await;
    assert!(pool.is_ok(), "Failed create connection redis pool");

    (pool.unwrap(), container)
}

pub struct Timer(std::time::Instant);

impl Timer {
    pub fn start()-> Self {
        Self(std::time::Instant::now())
    }

    pub fn stop(self) -> Duration {
        self.0.elapsed()
    }
}

pub struct ActivityBuilder {
    activity_type: String,
    payload: Option<serde_json::Value>,
    priority: Option<ActivityPriority>,
    max_retries: Option<u32>,
    timeout: Option<Duration>,
    delay: Option<Duration>,
}

impl ActivityBuilder {
    /// Creates a new ActivityBuilder for the given engine and activity type.
    pub fn new(activity_type: String) -> Self {
        Self {
            activity_type,
            payload: None,
            priority: None,
            max_retries: None,
            timeout: None,
            delay: None,
        }
    }

    /// Sets the payload for the activity.
    ///
    /// # Parameters
    ///
    /// * `payload` - JSON payload containing the activity data
    pub fn payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = Some(payload);
        self
    }

    /// Sets the priority for the activity.
    ///
    /// # Parameters
    ///
    /// * `priority` - Activity priority level
    pub fn priority(mut self, priority: ActivityPriority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Sets the maximum number of retries for the activity.
    ///
    /// # Parameters
    ///
    /// * `retries` - Maximum number of retry attempts (0 for unlimited)
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Sets the timeout for the activity execution.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Maximum execution time before timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the delay before the activity should be executed.
    ///
    /// # Parameters
    ///
    /// * `delay` - Delay before execution
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Executes the activity with the configured settings.
    ///
    /// # Returns
    ///
    /// Returns a `Result<Activity, WorkerError>` containing the activity future
    /// or an error if the activity cannot be enqueued.
    ///
    /// # Errors
    ///
    /// Returns `WorkerError` if the activity cannot be built for any reason.
    pub fn build(self) -> Result<Activity, WorkerError> {
        let payload = self.payload.ok_or_else(|| {
            WorkerError::QueueError("Activity payload is required".to_string())
        })?;

        let option = if self.priority.is_some() || self.max_retries.is_some() || self.timeout.is_some() || self.delay.is_some() {
            Some(ActivityOption {
                priority: self.priority,
                max_retries: self.max_retries.unwrap_or(3),
                timeout_seconds: self.timeout.map(|d| d.as_secs()).unwrap_or(300),
                delay_seconds: self.delay.map(|d| d.as_secs()),
            })
        } else {
            None
        };

        Ok(Activity::new(self.activity_type, payload, option))
    }
}

