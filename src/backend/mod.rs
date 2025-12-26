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
//! - [`QueueBackend`]: Core queue operations (enqueue, dequeue, ack)
//! - [`InspectionBackend`]: Observability operations (stats, listing, events)
//! - [`Backend`]: Combined super-trait for full-featured backends
//! - [`BackendError`]: Backend-agnostic error type
//!
//! # Using a Custom Backend
//!
//! ```rust,ignore
//! use runner_q::WorkerEngine;
//! use runner_q::backend::Backend;
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
pub mod redis;
mod traits;

// Re-export error type
pub use error::BackendError;

// Re-export trait types (includes re-exports from observability)
pub use traits::{
    // Observability types (re-exported from observability module)
    ActivityEvent,
    ActivityEventType,
    // Backend domain types
    ActivityResult,
    ActivitySnapshot,
    // Traits
    Backend,
    DeadLetterRecord,
    DequeuedActivity,
    FailureKind,
    IdempotencyBehavior,
    InspectionBackend,
    PriorityBreakdown,
    QueueBackend,
    QueueStats,
    QueuedActivity,
    ResultState,
};

// Re-export Redis backend
pub use redis::RedisBackend;
