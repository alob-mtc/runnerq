//! Observability module for queue inspection and monitoring.
//!
//! This module provides:
//! - [`QueueInspector`]: Read-only access to queue state
//! - [`ActivitySnapshot`], [`ActivityEvent`]: Core observability types
//! - [`DeadLetterRecord`]: Dead letter queue records
//! - UI components for web-based monitoring (with `axum-ui` feature)

#[cfg(any(feature = "redis", feature = "postgres"))]
pub mod inspector;
mod models;
pub mod ui;

// Re-export models
pub use models::{
    ActivityEvent, ActivityEventType, ActivitySnapshot, DeadLetterRecord, QueueStats,
};

// Re-export inspector (requires a backend feature)
#[cfg(any(feature = "redis", feature = "postgres"))]
pub use inspector::QueueInspector;

// Re-export static HTML for custom integrations
pub use ui::CONSOLE_HTML;

// Re-export UI routes (requires axum-ui feature and a backend)
#[cfg(all(feature = "axum-ui", any(feature = "redis", feature = "postgres")))]
pub use ui::{observability_api, runnerq_ui};
