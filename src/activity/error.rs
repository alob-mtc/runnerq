/// Error type for activity operations that supports retry semantics.
///
/// This enum distinguishes between errors that should trigger a retry
/// and those that indicate permanent failure.
///
/// # Examples
///
/// ```rust
/// use runner_q::ActivityError;
/// use runner_q::ActivityHandlerResult;
///
/// // Retryable error - temporary network issue
/// let retry_error = ActivityError::Retry("Network timeout".to_string());
///
/// // Non-retryable error - invalid input data
/// let permanent_error = ActivityError::NonRetry("Invalid user ID format".to_string());
///
/// // Using in activity handlers
/// fn process_user_data(payload: serde_json::Value) -> ActivityHandlerResult {
///     let user_id = payload["user_id"].as_str()
///         .ok_or_else(|| ActivityError::NonRetry("Missing user_id".to_string()))?;
///     
///     if user_id.is_empty() {
///         return Err(ActivityError::NonRetry("Empty user_id".to_string()));
///     }
///     
///     // Simulate processing that might fail temporarily
///     if payload["retry_processing"].as_bool().unwrap_or(false) {
///         Err(ActivityError::Retry("Processing failed, will retry".to_string()))
///     } else {
///         Ok(Some(serde_json::json!({"processed": true})))
///     }
/// }
/// ```
#[derive(Debug)]
pub enum ActivityError {
    /// Error that should trigger a retry attempt.
    ///
    /// Use this for temporary failures like network timeouts, temporary service
    /// unavailability, or other transient issues that might resolve on retry.
    Retry(String),

    /// Error that should not trigger a retry.
    ///
    /// Use this for permanent failures like invalid input data, authentication
    /// errors, or other issues that won't be resolved by retrying.
    NonRetry(String),
}

impl std::fmt::Display for ActivityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityError::Retry(msg) => write!(f, "Retryable error: {}", msg),
            ActivityError::NonRetry(msg) => write!(f, "Non-retryable error: {}", msg),
        }
    }
}

impl std::error::Error for ActivityError {}

impl From<serde_json::Error> for ActivityError {
    fn from(error: serde_json::Error) -> Self {
        if error.is_retryable() {
            ActivityError::Retry(format!("JSON error: {}", error))
        } else {
            ActivityError::NonRetry(format!("JSON error: {}", error))
        }
    }
}

impl From<std::io::Error> for ActivityError {
    fn from(error: std::io::Error) -> Self {
        if error.is_retryable() {
            ActivityError::Retry(format!("IO error: {}", error))
        } else {
            ActivityError::NonRetry(format!("IO error: {}", error))
        }
    }
}

/// Trait to determine if an error should be retried.
///
/// This trait allows custom error types to specify their retry behavior,
/// enabling automatic conversion to `ActivityError` with appropriate retry semantics.
///
/// # Examples
///
/// ```rust
/// use runner_q::RetryableError;
/// use runner_q::ActivityError;
///
/// // Custom error type
/// #[derive(Debug)]
/// pub struct DatabaseError {
///     message: String,
///     is_connection_error: bool,
/// }
///
/// impl RetryableError for DatabaseError {
///     fn is_retryable(&self) -> bool {
///         self.is_connection_error
///     }
/// }
///
/// impl From<DatabaseError> for ActivityError {
///     fn from(err: DatabaseError) -> Self {
///         if err.is_retryable() {
///             ActivityError::Retry(err.message)
///         } else {
///             ActivityError::NonRetry(err.message)
///         }
///     }
/// }
///
/// // Usage in activity handler
/// fn handle_database_operation() -> Result<(), DatabaseError> {
///     // ... database operation that might fail
///     Err(DatabaseError {
///         message: "Connection timeout".to_string(),
///         is_connection_error: true,
///     })
/// }
/// ```
pub trait RetryableError {
    /// Determine if this error should trigger a retry attempt.
    ///
    /// Return `true` for temporary errors that might resolve on retry,
    /// `false` for permanent errors that won't be fixed by retrying.
    fn is_retryable(&self) -> bool;
}

// Implement RetryableError for common error types

/// Implementation for std::io::Error
impl RetryableError for std::io::Error {
    fn is_retryable(&self) -> bool {
        match self.kind() {
            std::io::ErrorKind::TimedOut => true,
            std::io::ErrorKind::Interrupted => true,
            std::io::ErrorKind::WouldBlock => true,
            std::io::ErrorKind::ConnectionRefused => true,
            std::io::ErrorKind::ConnectionAborted => true,
            std::io::ErrorKind::ConnectionReset => true,
            std::io::ErrorKind::PermissionDenied => false,
            std::io::ErrorKind::NotFound => false,
            std::io::ErrorKind::AlreadyExists => false,
            _ => true, // Default to retryable for other IO errors
        }
    }
}

/// Implementation for serde_json::Error
impl RetryableError for serde_json::Error {
    fn is_retryable(&self) -> bool {
        // JSON parsing errors are typically not retryable
        false
    }
}

#[cfg(test)]
mod example_handlers {
    use super::*;
    use crate::activity::activity::{ActivityContext, ActivityHandler};
    use crate::ActivityHandlerResult;
    use async_trait::async_trait;
    use serde_json::json;

    /// Example handler demonstrating ? operator usage pattern
    pub struct FileProcessingActivity;

    #[async_trait]
    impl ActivityHandler for FileProcessingActivity {
        async fn handle(
            &self,
            payload: serde_json::Value,
            _context: ActivityContext,
        ) -> ActivityHandlerResult {
            // Extract file path with ? operator
            let file_path = payload["file_path"]
                .as_str()
                .ok_or(ActivityError::NonRetry(
                    "Missing file_path in payload".to_string(),
                ))?;

            // Validate input
            if file_path.is_empty() {
                return Err(ActivityError::NonRetry("Empty file path".to_string()));
            }

            // Read file content - convert IO error to ActivityError
            let content = std::fs::read_to_string(file_path).map_err(|e| {
                if e.is_retryable() {
                    ActivityError::Retry(e.to_string())
                } else {
                    ActivityError::NonRetry(e.to_string())
                }
            })?;

            // Parse as JSON - convert serde error to ActivityError
            let json_content: serde_json::Value = serde_json::from_str(&content)
                .map_err(|e| ActivityError::NonRetry(e.to_string()))?;

            // Process the data
            Ok(Some(json!({
                "original_file": file_path,
                "content_length": content.len(),
                "json_keys": json_content.as_object()
                    .map(|obj| obj.keys().collect::<Vec<_>>())
                    .unwrap_or_default(),
                "processed_at": chrono::Utc::now().to_rfc3339()
            })))
        }

        fn activity_type(&self) -> String {
            "file_processing_example".to_string()
        }
    }
}
