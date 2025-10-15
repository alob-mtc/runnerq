use redis::RedisError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("{0}")]
    CustomError(String),

    #[error("Activity queue error: {0}")]
    QueueError(String),

    #[error("Activity serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Activity execution timeout")]
    Timeout,

    #[error("Activity execution failed: {0}")]
    ExecutionError(String),

    #[error("Activity handler not found for activity type: {0}")]
    HandlerNotFound(String),

    #[error("Redis connection error: {0}")]
    RedisError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Worker shutdown requested")]
    Shutdown,

    #[error("Worker is already running")]
    AlreadyRunning,

    #[error("Activity scheduling error: {0}")]
    SchedulingError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<RedisError> for WorkerError {
    fn from(err: RedisError) -> Self {
        WorkerError::RedisError(err.to_string())
    }
}

impl WorkerError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, WorkerError::QueueError(_) | WorkerError::RedisError(_) | WorkerError::DatabaseError(_) | WorkerError::Timeout | WorkerError::ExecutionError(_) | WorkerError::SchedulingError(_))
    }
}
