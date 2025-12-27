//! Observability module for queue inspection and monitoring.
//!
//! This module provides:
//! - [`QueueInspector`]: Read-only access to queue state (requires `redis` feature)
//! - [`ActivitySnapshot`], [`ActivityEvent`]: Core observability types
//! - [`DeadLetterRecord`]: Dead letter queue records
//! - UI components for web-based monitoring

#[cfg(feature = "redis")]
pub mod inspector;
mod models;
#[cfg(feature = "redis")]
pub mod ui;

// Re-export models
pub use models::{
    ActivityEvent, ActivityEventType, ActivitySnapshot, DeadLetterRecord, QueueStats,
};

// Re-export inspector (requires redis feature)
#[cfg(feature = "redis")]
pub use inspector::QueueInspector;

// Re-export UI routes (requires redis feature)
#[cfg(feature = "redis")]
pub use ui::{observability_api, runnerq_ui};
