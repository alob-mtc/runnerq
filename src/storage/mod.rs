//! Backend abstraction layer for RunnerQ.
//!
//! This module provides traits and implementations for different storage backends.
//! The built-in backend is PostgreSQL; you can also use the `runner_q_redis` crate
//! or implement the storage traits for custom backends.
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
//! # Using the PostgreSQL backend
//!
//! ```rust,ignore
//! use runner_q::{WorkerEngine, storage::PostgresBackend};
//! use std::sync::Arc;
//!
//! let backend = PostgresBackend::new("postgres://localhost/mydb", "my_app").await?;
//! let engine = WorkerEngine::builder()
//!     .backend(Arc::new(backend))
//!     .queue_name("my_app")
//!     .build()
//!     .await?;
//! ```

mod error;
mod traits;

// PostgreSQL backend
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

// Re-export PostgreSQL backend
#[cfg(feature = "postgres")]
pub use postgres::PostgresBackend;
