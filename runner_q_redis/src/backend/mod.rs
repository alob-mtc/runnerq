//! Redis backend implementation for RunnerQ.

mod inspection;
mod pool;
mod queue;

use std::time::Duration;

/// Maps `redis::RedisError` to `StorageError` (runner_q_redis owns this conversion).
pub(crate) fn redis_to_storage(e: redis::RedisError) -> StorageError {
    StorageError::Unavailable(e.to_string())
}

use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use futures::stream::BoxStream;
use runner_q::storage::{
    ActivityEvent, ActivityResult, ActivitySnapshot, DeadLetterRecord, FailureKind,
    InspectionStorage, QueueStats, QueueStorage, QueuedActivity, ResultStorage, StorageError,
};
use uuid::Uuid;

pub use pool::RedisConfig;
pub use pool::{create_redis_pool, create_redis_pool_with_config};

/// Redis-based backend for RunnerQ.
#[derive(Clone)]
pub struct RedisBackend {
    pool: Pool<RedisConnectionManager>,
    queue_name: String,
    default_lease_ms: u64,
}

impl RedisBackend {
    /// Create a new Redis backend with the given pool and queue name.
    pub fn new(pool: Pool<RedisConnectionManager>, queue_name: String) -> Self {
        Self {
            pool,
            queue_name,
            default_lease_ms: 60_000,
        }
    }

    /// Create a new Redis backend with custom lease duration.
    pub fn with_lease_ms(mut self, lease_ms: u64) -> Self {
        self.default_lease_ms = lease_ms;
        self
    }

    /// Create a builder for configuring the Redis backend.
    pub fn builder() -> RedisBackendBuilder {
        RedisBackendBuilder::new()
    }

    /// Get the Redis connection pool.
    pub fn pool(&self) -> &Pool<RedisConnectionManager> {
        &self.pool
    }

    /// Get the queue name.
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Get the default lease duration in milliseconds.
    pub fn default_lease_ms(&self) -> u64 {
        self.default_lease_ms
    }

    fn main_queue_key(&self) -> String {
        format!("{}:priority_queue", self.queue_name)
    }

    fn processing_queue_key(&self) -> String {
        format!("{}:processing", self.queue_name)
    }

    fn scheduled_key(&self) -> String {
        format!("{}:scheduled_activities", self.queue_name)
    }

    fn dead_letter_key(&self) -> String {
        format!("{}:dead_letter_queue", self.queue_name)
    }

    fn completed_key(&self) -> String {
        format!("{}:completed_activities", self.queue_name)
    }

    fn events_stream_key(&self) -> String {
        format!("{}:events_stream", self.queue_name)
    }

    fn activity_key(activity_id: &Uuid) -> String {
        format!("activity:{}", activity_id)
    }

    fn activity_events_key(activity_id: &Uuid) -> String {
        format!("activity:{}:events", activity_id)
    }

    fn result_key(activity_id: &Uuid) -> String {
        format!("result:{}", activity_id)
    }

    fn idempotency_key(&self, key: &str) -> String {
        format!("{}:idempotency:{}", self.queue_name, key)
    }
}

/// Builder for creating a Redis backend with custom configuration.
pub struct RedisBackendBuilder {
    redis_url: Option<String>,
    queue_name: Option<String>,
    config: Option<RedisConfig>,
    lease_ms: Option<u64>,
}

impl RedisBackendBuilder {
    pub fn new() -> Self {
        Self {
            redis_url: None,
            queue_name: None,
            config: None,
            lease_ms: None,
        }
    }

    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.redis_url = Some(url.into());
        self
    }

    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = Some(name.into());
        self
    }

    pub fn config(mut self, config: RedisConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn lease_ms(mut self, ms: u64) -> Self {
        self.lease_ms = Some(ms);
        self
    }

    pub async fn build(self) -> Result<RedisBackend, StorageError> {
        let redis_url = self
            .redis_url
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
        let queue_name = self.queue_name.unwrap_or_else(|| "default".to_string());
        let config = self.config.unwrap_or_default();

        let pool = create_redis_pool_with_config(&redis_url, config).await?;

        let mut backend = RedisBackend::new(pool, queue_name);
        if let Some(lease_ms) = self.lease_ms {
            backend = backend.with_lease_ms(lease_ms);
        }

        Ok(backend)
    }
}

impl Default for RedisBackendBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ResultStorage for RedisBackend {
    async fn get_result(&self, activity_id: Uuid) -> Result<Option<ActivityResult>, StorageError> {
        queue::get_result(self, activity_id).await
    }
}

#[async_trait]
impl QueueStorage for RedisBackend {
    async fn enqueue(&self, activity: QueuedActivity) -> Result<(), StorageError> {
        queue::enqueue(self, activity).await
    }

    async fn dequeue(
        &self,
        worker_id: &str,
        timeout: Duration,
        _activity_types: Option<&[String]>,
    ) -> Result<Option<QueuedActivity>, StorageError> {
        queue::dequeue(self, worker_id, timeout).await
    }

    async fn ack_success(
        &self,
        activity_id: Uuid,
        result: Option<serde_json::Value>,
        worker_id: &str,
    ) -> Result<(), StorageError> {
        queue::ack_success(self, activity_id, result, worker_id).await
    }

    async fn ack_failure(
        &self,
        activity_id: Uuid,
        failure: FailureKind,
        worker_id: &str,
    ) -> Result<bool, StorageError> {
        queue::ack_failure(self, activity_id, failure, worker_id).await
    }

    async fn process_scheduled(&self) -> Result<u64, StorageError> {
        queue::process_scheduled(self).await
    }

    async fn requeue_expired(&self, batch_size: usize) -> Result<u64, StorageError> {
        queue::requeue_expired(self, batch_size).await
    }

    async fn extend_lease(
        &self,
        activity_id: Uuid,
        extend_by: Duration,
    ) -> Result<bool, StorageError> {
        queue::extend_lease(self, activity_id, extend_by).await
    }

    async fn store_result(
        &self,
        activity_id: Uuid,
        result: ActivityResult,
    ) -> Result<(), StorageError> {
        queue::store_result(self, activity_id, result).await
    }

    async fn check_idempotency(
        &self,
        activity: &QueuedActivity,
    ) -> Result<Option<Uuid>, StorageError> {
        queue::check_idempotency(self, activity).await
    }
}

#[async_trait]
impl InspectionStorage for RedisBackend {
    async fn stats(&self) -> Result<QueueStats, StorageError> {
        inspection::stats(self).await
    }

    async fn list_pending(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        inspection::list_pending(self, offset, limit).await
    }

    async fn list_processing(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        inspection::list_processing(self, offset, limit).await
    }

    async fn list_scheduled(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        inspection::list_scheduled(self, offset, limit).await
    }

    async fn list_completed(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, StorageError> {
        inspection::list_completed(self, offset, limit).await
    }

    async fn list_dead_letter(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, StorageError> {
        inspection::list_dead_letter(self, offset, limit).await
    }

    async fn get_activity(
        &self,
        activity_id: Uuid,
    ) -> Result<Option<ActivitySnapshot>, StorageError> {
        inspection::get_activity(self, activity_id).await
    }

    async fn get_activity_events(
        &self,
        activity_id: Uuid,
        limit: usize,
    ) -> Result<Vec<ActivityEvent>, StorageError> {
        inspection::get_activity_events(self, activity_id, limit).await
    }

    fn event_stream(&self) -> BoxStream<'static, Result<ActivityEvent, StorageError>> {
        inspection::event_stream(self)
    }
}
