//! Observability module for queue inspection and monitoring.
//!
//! This module provides:
//! - [`QueueInspector`]: Read-only access to queue state
//! - [`ActivitySnapshot`], [`ActivityEvent`]: Core observability types
//! - [`DeadLetterRecord`]: Dead letter queue records
//! - UI components for web-based monitoring

#[cfg(any(feature = "redis", feature = "postgres"))]
pub mod inspector;
mod models;
#[cfg(any(feature = "redis", feature = "postgres"))]
pub mod ui;

// Re-export models
pub use models::{
    ActivityEvent, ActivityEventType, ActivitySnapshot, DeadLetterRecord, QueueStats,
};

// Re-export inspector (requires a backend feature)
#[cfg(any(feature = "redis", feature = "postgres"))]
pub use inspector::QueueInspector;

// Re-export UI routes (requires a backend feature)
#[cfg(any(feature = "redis", feature = "postgres"))]
pub use ui::{observability_api, runnerq_ui};
