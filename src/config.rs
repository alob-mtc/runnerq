use serde::{Deserialize, Serialize};

/// Configuration for the WorkerEngine that controls queue behavior and resource usage.
///
/// This struct contains all the necessary settings to configure how the worker engine
/// processes activities and manages concurrency. The storage backend is provided
/// separately via `WorkerEngine::builder().backend(...)`.
///
/// # Examples
///
/// ```rust
/// use runner_q::WorkerConfig;
///
/// let config = WorkerConfig {
///     queue_name: "my_app_queue".to_string(),
///     max_concurrent_activities: 5,
///     ..WorkerConfig::default()
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// The name of the queue to use for activities.
    ///
    /// This name is used as a prefix to avoid conflicts between different
    /// applications or environments (e.g. table/stream names in the backend).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use runner_q::WorkerConfig;
    ///
    /// let config = WorkerConfig {
    ///     queue_name: "production_activities".to_string(),
    ///     // ... other fields
    ///     ..WorkerConfig::default()
    /// };
    /// ```
    pub queue_name: String,

    /// Maximum number of activities that can be processed concurrently.
    ///
    /// This controls the worker pool size and prevents resource exhaustion.
    /// Higher values allow more parallelism but consume more memory and CPU.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use runner_q::WorkerConfig;
    ///
    /// // Conservative setting for resource-constrained environments
    /// let config = WorkerConfig {
    ///     max_concurrent_activities: 2,
    ///     // ... other fields
    ///     ..WorkerConfig::default()
    /// };
    ///
    /// // High-throughput setting for powerful servers
    /// let config = WorkerConfig {
    ///     max_concurrent_activities: 50,
    ///     // ... other fields
    ///     ..WorkerConfig::default()
    /// };
    /// ```
    pub max_concurrent_activities: usize,

    /// Interval in seconds for polling scheduled activities.
    ///
    /// When `None`, uses a default interval of 30 seconds.
    /// Lower values provide more responsive scheduling but increase backend load.
    ///
    /// This setting only takes effect for backends that do **not** handle
    /// scheduling natively in `dequeue()`. The PostgreSQL backend handles
    /// scheduled activities directly in `dequeue()` and skips the polling loop.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use runner_q::WorkerConfig;
    ///
    /// // Check for scheduled activities every 10 seconds
    /// let config = WorkerConfig {
    ///     schedule_poll_interval_seconds: Some(10),
    ///     // ... other fields
    ///     ..WorkerConfig::default()
    /// };
    ///
    /// // Use default polling interval (30 seconds)
    /// let config = WorkerConfig {
    ///     schedule_poll_interval_seconds: None,
    ///     // ... other fields
    ///     ..WorkerConfig::default()
    /// };
    /// ```
    pub schedule_poll_interval_seconds: Option<u64>,

    /// Lease duration in milliseconds for claimed activities before considered expired
    /// Defaults to 60000 ms (60s)
    pub lease_ms: Option<u64>,

    /// Interval in seconds for the reaper to scan processing leases
    /// Defaults to 5 seconds
    pub reaper_interval_seconds: Option<u64>,

    /// Maximum number of expired items to requeue per reaper tick
    /// Defaults to 100
    pub reaper_batch_size: Option<usize>,

    /// Optional filter to restrict this engine to specific activity types.
    ///
    /// When `Some`, workers will only dequeue activities whose `activity_type`
    /// matches one of the listed values. When `None` (the default), workers
    /// dequeue all activity types (catch-all).
    pub activity_types: Option<Vec<String>>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            max_concurrent_activities: 10,
            schedule_poll_interval_seconds: None,
            lease_ms: Some(60_000),
            reaper_interval_seconds: Some(5),
            reaper_batch_size: Some(100),
            activity_types: None,
        }
    }
}
