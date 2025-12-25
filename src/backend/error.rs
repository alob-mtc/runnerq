//! Backend-agnostic error types for RunnerQ queue backends.
//!
//! These error types provide a consistent interface for all backend implementations,
//! allowing the engine to handle errors uniformly regardless of the underlying storage.

use thiserror::Error;

/// Errors that can occur during backend operations.
///
/// This enum provides backend-agnostic error variants that all implementations
/// should map their internal errors to. This allows the `WorkerEngine` and
/// other components to handle errors consistently.
///
/// # Examples
///
/// ```rust
/// use runner_q::backend::BackendError;
///
/// fn handle_error(err: BackendError) {
///     match err {
///         BackendError::Unavailable(msg) => {
///             // Connection lost, retry later
///             eprintln!("Backend unavailable: {}", msg);
///         }
///         BackendError::NotFound(msg) => {
///             // Resource doesn't exist
///             eprintln!("Not found: {}", msg);
///         }
///         _ => {
///             eprintln!("Error: {}", err);
///         }
///     }
/// }
/// ```
#[derive(Error, Debug)]
pub enum BackendError {
    /// Backend is unavailable (connection lost, service down, etc.)
    #[error("backend unavailable: {0}")]
    Unavailable(String),

    /// Conflict during operation (e.g., concurrent modification)
    #[error("conflict: {0}")]
    Conflict(String),

    /// Requested resource was not found
    #[error("not found: {0}")]
    NotFound(String),

    /// Internal backend error
    #[error("internal error: {0}")]
    Internal(String),

    /// Serialization/deserialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Timeout during operation
    #[error("operation timeout: {0}")]
    Timeout(String),

    /// Duplicate activity detected (idempotency violation)
    #[error("duplicate activity: {0}")]
    DuplicateActivity(String),

    /// Idempotency key conflict
    #[error("idempotency conflict: {0}")]
    IdempotencyConflict(String),
}

impl BackendError {
    /// Returns true if this error is potentially recoverable with a retry.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            BackendError::Unavailable(_) | BackendError::Timeout(_) | BackendError::Conflict(_)
        )
    }
}

// Conversion from serde_json errors
impl From<serde_json::Error> for BackendError {
    fn from(err: serde_json::Error) -> Self {
        BackendError::Serialization(err.to_string())
    }
}

// Conversion from Redis errors
impl From<redis::RedisError> for BackendError {
    fn from(err: redis::RedisError) -> Self {
        BackendError::Unavailable(err.to_string())
    }
}
