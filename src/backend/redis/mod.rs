//! Redis backend implementation for RunnerQ.
//!
//! This module provides a Redis-based implementation of the [`Backend`] trait,
//! which is the default backend for RunnerQ.
//!
//! # Features
//!
//! - Priority-based queue using Redis sorted sets
//! - Lease-based activity claiming with automatic expiration
//! - Scheduled activity support using sorted sets
//! - Dead letter queue for failed activities
//! - Real-time event streaming via Redis Streams
//! - Activity snapshots for observability
//!
//! # Usage
//!
//! ```rust,ignore
//! use runner_q::backend::RedisBackend;
//! use runner_q::WorkerEngine;
//! use std::sync::Arc;
//!
//! // Create Redis backend with custom configuration
//! let backend = RedisBackend::builder()
//!     .redis_url("redis://localhost:6379")
//!     .queue_name("my_app")
//!     .build()
//!     .await?;
//!
//! let engine = WorkerEngine::builder()
//!     .backend(Arc::new(backend))
//!     .max_workers(8)
//!     .build()
//!     .await?;
//! ```
//!
//! # Valkey Compatibility
//!
//! Since Valkey is Redis-compatible, this backend works with Valkey servers
//! by simply pointing the URL to a Valkey instance.

mod inspection;
mod pool;
mod queue;

use std::time::Duration;

use bb8_redis::{bb8::Pool, RedisConnectionManager};

pub use pool::RedisConfig;
pub use pool::{create_redis_pool, create_redis_pool_with_config};

use super::error::BackendError;
use super::traits::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use uuid::Uuid;

/// Redis-based backend for RunnerQ.
///
/// This struct provides a complete implementation of both [`QueueBackend`]
/// and [`InspectionBackend`] using Redis as the storage layer.
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
            default_lease_ms: 60_000, // 60 seconds default lease
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

    // Key helpers
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
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            redis_url: None,
            queue_name: None,
            config: None,
            lease_ms: None,
        }
    }

    /// Set the Redis URL.
    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.redis_url = Some(url.into());
        self
    }

    /// Set the queue name (used as key prefix).
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = Some(name.into());
        self
    }

    /// Set the Redis pool configuration.
    pub fn config(mut self, config: RedisConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the default lease duration in milliseconds.
    pub fn lease_ms(mut self, ms: u64) -> Self {
        self.lease_ms = Some(ms);
        self
    }

    /// Build the Redis backend.
    pub async fn build(self) -> Result<RedisBackend, BackendError> {
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

// ============================================================================
// QueueBackend Implementation
// ============================================================================

#[async_trait]
impl QueueBackend for RedisBackend {
    async fn enqueue(&self, activity: QueuedActivity) -> Result<(), BackendError> {
        queue::enqueue(self, activity).await
    }

    async fn dequeue(
        &self,
        worker_id: &str,
        timeout: Duration,
    ) -> Result<Option<DequeuedActivity>, BackendError> {
        queue::dequeue(self, worker_id, timeout).await
    }

    async fn ack_success(
        &self,
        activity_id: Uuid,
        lease_id: &str,
        result: Option<serde_json::Value>,
        worker_id: &str,
    ) -> Result<(), BackendError> {
        queue::ack_success(self, activity_id, lease_id, result, worker_id).await
    }

    async fn ack_failure(
        &self,
        activity_id: Uuid,
        lease_id: &str,
        failure: FailureKind,
        worker_id: &str,
    ) -> Result<bool, BackendError> {
        queue::ack_failure(self, activity_id, lease_id, failure, worker_id).await
    }

    async fn process_scheduled(&self) -> Result<u64, BackendError> {
        queue::process_scheduled(self).await
    }

    async fn requeue_expired(&self, batch_size: usize) -> Result<u64, BackendError> {
        queue::requeue_expired(self, batch_size).await
    }

    async fn extend_lease(
        &self,
        activity_id: Uuid,
        extend_by: Duration,
    ) -> Result<bool, BackendError> {
        queue::extend_lease(self, activity_id, extend_by).await
    }

    async fn store_result(
        &self,
        activity_id: Uuid,
        result: ActivityResult,
    ) -> Result<(), BackendError> {
        queue::store_result(self, activity_id, result).await
    }

    async fn get_result(&self, activity_id: Uuid) -> Result<Option<ActivityResult>, BackendError> {
        queue::get_result(self, activity_id).await
    }

    async fn check_idempotency(
        &self,
        activity: &QueuedActivity,
    ) -> Result<Option<Uuid>, BackendError> {
        queue::check_idempotency(self, activity).await
    }
}

// ============================================================================
// InspectionBackend Implementation
// ============================================================================

#[async_trait]
impl InspectionBackend for RedisBackend {
    async fn stats(&self) -> Result<QueueStats, BackendError> {
        inspection::stats(self).await
    }

    async fn list_pending(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, BackendError> {
        inspection::list_pending(self, offset, limit).await
    }

    async fn list_processing(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, BackendError> {
        inspection::list_processing(self, offset, limit).await
    }

    async fn list_scheduled(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, BackendError> {
        inspection::list_scheduled(self, offset, limit).await
    }

    async fn list_completed(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ActivitySnapshot>, BackendError> {
        inspection::list_completed(self, offset, limit).await
    }

    async fn list_dead_letter(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, BackendError> {
        inspection::list_dead_letter(self, offset, limit).await
    }

    async fn get_activity(
        &self,
        activity_id: Uuid,
    ) -> Result<Option<ActivitySnapshot>, BackendError> {
        inspection::get_activity(self, activity_id).await
    }

    async fn get_activity_events(
        &self,
        activity_id: Uuid,
        limit: usize,
    ) -> Result<Vec<ActivityEvent>, BackendError> {
        inspection::get_activity_events(self, activity_id, limit).await
    }

    fn event_stream(&self) -> BoxStream<'static, Result<ActivityEvent, BackendError>> {
        inspection::event_stream(self)
    }
}
