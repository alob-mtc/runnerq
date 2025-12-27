//! Observability module for queue inspection and monitoring.
//!
//! This module provides:
//! - [`QueueInspector`]: Read-only access to queue state
//! - [`ActivitySnapshot`], [`ActivityEvent`]: Core observability types
//! - [`DeadLetterRecord`]: Dead letter queue records
//! - UI components for web-based monitoring

pub mod inspector;
mod models;
pub mod ui;

// Re-export models
pub use models::{
    ActivityEvent, ActivityEventType, ActivitySnapshot, DeadLetterRecord, QueueStats,
};

// Re-export inspector
pub use inspector::QueueInspector;

// Re-export UI routes
pub use ui::{observability_api, runnerq_ui};
