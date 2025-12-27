//! Backend abstraction layer for RunnerQ.
//!
//! This module provides traits and implementations for different storage backends.
//! The default backend is Redis, but the abstraction allows for other implementations
//! such as Valkey, Kafka, or SQL-based backends.
//!
//! # Architecture
//!
//! The backend abstraction consists of:
//!
//! - [`QueueStorage`]: Core queue operations (enqueue, dequeue, ack)
//! - [`InspectionStorage`]: Observability operations (stats, listing, events)
//! - [`Storage`]: Combined super-trait for full-featured backends
//! - [`StorageError`]: Backend-agnostic error type
//!
//! # Using a Custom Backend
//!
//! ```rust,ignore
//! use runner_q::WorkerEngine;
//! use runner_q::storage::Backend;
//! use std::sync::Arc;
//!
//! // Your custom backend implementation
//! let backend: Arc<dyn Backend> = Arc::new(MyCustomBackend::new(config));
//!
//! let engine = WorkerEngine::builder()
//!     .backend(backend)
//!     .max_workers(8)
//!     .build()
//!     .await?;
//! ```
//!
//! # Using the Default Redis Backend
//!
//! ```rust,ignore
//! use runner_q::WorkerEngine;
//!
//! // Redis backend is used by default when you specify redis_url
//! let engine = WorkerEngine::builder()
//!     .redis_url("redis://localhost:6379")
//!     .queue_name("my_app")
//!     .build()
//!     .await?;
//! ```

mod error;
mod traits;

// Redis backend
#[cfg(feature = "redis")]
pub mod redis;

// PostgreSQL backend (in development)
#[cfg(feature = "postgres")]
pub mod postgres;

// Re-export error type
pub use error::StorageError;

// Re-export trait types (includes re-exports from observability)
pub use traits::{
    // Observability types (re-exported from observability module)
    ActivityEvent,
    ActivityEventType,
    // Backend domain types
    ActivityResult,
    ActivitySnapshot,
    DeadLetterRecord,
    DequeuedActivity,
    FailureKind,
    IdempotencyBehavior,
    InspectionStorage,
    PriorityBreakdown,
    QueueStats,
    QueueStorage,
    QueuedActivity,
    ResultState,
    ResultStorage,
    // Traits
    Storage,
};

// Re-export Redis backend
#[cfg(feature = "redis")]
pub use redis::RedisBackend;

// Re-export PostgreSQL backend (in development)
#[cfg(feature = "postgres")]
pub use postgres::PostgresBackend;
