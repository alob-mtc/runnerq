//! Redis storage backend for [RunnerQ](https://github.com/alob-mtc/runnerq).
//!
//! Use this crate when you want to use Redis (or a Redis-compatible server like Valkey)
//! as the storage backend for RunnerQ.
//!
//! # Example
//!
//! ```rust,ignore
//! use runner_q::WorkerEngine;
//! use runner_q_redis::RedisBackend;
//! use std::sync::Arc;
//!
//! let backend = RedisBackend::builder()
//!     .redis_url("redis://localhost:6379")
//!     .queue_name("my_app")
//!     .build()
//!     .await?;
//!
//! let engine = WorkerEngine::builder()
//!     .backend(Arc::new(backend))
//!     .queue_name("my_app")
//!     .max_workers(8)
//!     .build()
//!     .await?;
//! ```

mod backend;

pub use backend::{
    RedisBackend, RedisBackendBuilder, RedisConfig, create_redis_pool, create_redis_pool_with_config,
};
