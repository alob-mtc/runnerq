//! UI adapters for the RunnerQ Console.
//!
//! This module provides HTTP framework adapters for serving the observability
//! API and Console UI. Currently supports:
//!
//! - **Axum** (enabled by default with `axum-ui` feature)
//!
//! # Feature Flags
//!
//! - `axum-ui`: Enables Axum-based HTTP endpoints (default)
//!
//! # Using with Other Frameworks
//!
//! If you're using a different HTTP framework, you can:
//! 1. Use the [`CONSOLE_HTML`] constant to serve the static UI
//! 2. Use [`QueueInspector`](crate::observability::QueueInspector) directly to build your own endpoints
//!
//! See GitHub issues for Actix-web and Warp support.

mod html;

#[cfg(feature = "axum-ui")]
mod axum;

// Re-export HTML for custom integrations
pub use html::CONSOLE_HTML;

// Re-export framework-specific implementations
#[cfg(feature = "axum-ui")]
pub use self::axum::{observability_api, runnerq_ui};

