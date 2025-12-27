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
/// use runner_q::storage::StorageError;
///
/// fn handle_error(err: StorageError) {
///     match err {
///         StorageError::Unavailable(msg) => {
///             // Connection lost, retry later
///             eprintln!("Backend unavailable: {}", msg);
///         }
///         StorageError::NotFound(msg) => {
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
pub enum StorageError {
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

impl StorageError {
    /// Returns true if this error is potentially recoverable with a retry.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StorageError::Unavailable(_) | StorageError::Timeout(_) | StorageError::Conflict(_)
        )
    }
}

// Conversion from serde_json errors
impl From<serde_json::Error> for StorageError {
    fn from(err: serde_json::Error) -> Self {
        StorageError::Serialization(err.to_string())
    }
}

// Conversion from Redis errors
#[cfg(feature = "redis")]
impl From<redis::RedisError> for StorageError {
    fn from(err: redis::RedisError) -> Self {
        StorageError::Unavailable(err.to_string())
    }
}
