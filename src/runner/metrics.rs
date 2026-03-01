// Activity lifecycle counters
pub const ACTIVITY_ENQUEUED: &str           = "runnerq_activity_enqueued_total";
pub const ACTIVITY_STARTED: &str            = "runnerq_activity_started_total";
pub const ACTIVITY_COMPLETED: &str          = "runnerq_activity_completed_total";
pub const ACTIVITY_RETRY: &str              = "runnerq_activity_retry_total";
pub const ACTIVITY_FAILED_NON_RETRY: &str   = "runnerq_activity_failed_non_retry_total";
pub const ACTIVITY_TIMEOUT: &str            = "runnerq_activity_timeout_total";
pub const ACTIVITY_DEAD_LETTERED: &str      = "runnerq_activity_dead_lettered_total";
pub const ACTIVITY_PANIC: &str              = "runnerq_activity_panic_total";
pub const ACTIVITY_HANDLER_NOT_FOUND: &str  = "runnerq_activity_handler_not_found_total";

// Queue / storage error paths
pub const ACTIVITY_ENQUEUE_ERROR: &str      = "runnerq_activity_enqueue_error_total";
pub const ACTIVITY_DEQUEUE_ERROR: &str      = "runnerq_activity_dequeue_error_total";
pub const RESULT_STORE_ERROR: &str          = "runnerq_activity_result_store_error_total";
pub const BACKEND_ERROR: &str               = "runnerq_backend_error_total";

// Worker-level
pub const WORKER_STARTED: &str              = "runnerq_worker_started_total";
pub const WORKER_STOPPED: &str              = "runnerq_worker_stopped_total";
pub const WORKER_CONCURRENCY_SATURATED: &str= "runnerq_worker_concurrency_saturated_total";

// Scheduler / reaper loops
pub const SCHEDULER_TICK: &str              = "runnerq_scheduler_tick_total";
pub const SCHEDULER_POLL_ERROR: &str        = "runnerq_scheduler_poll_error_total";
pub const REAPER_TICK: &str                 = "runnerq_reaper_tick_total";
pub const REAPER_ERROR: &str                = "runnerq_reaper_error_total";

// Durations (histograms / summaries)
pub const ACTIVITY_EXECUTION_DURATION: &str = "runnerq_activity_execution_seconds";
pub const ACTIVITY_QUEUE_LATENCY: &str      = "runnerq_activity_queue_latency_seconds";
pub const DEQUEUE_ROUNDTRIP_DURATION: &str  = "runnerq_dequeue_roundtrip_seconds";