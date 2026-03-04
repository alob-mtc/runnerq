# runner_q_redis

Redis storage backend for [RunnerQ](https://github.com/alob-mtc/runnerq). Use this crate when you want to use Redis (or a Redis-compatible server like Valkey) as the queue storage instead of the built-in PostgreSQL backend.

## Usage

Add both crates to your `Cargo.toml`:

```toml
[dependencies]
runner_q = "0.*"
runner_q_redis = "0.*"
```

Create a backend and pass it to the engine:

```rust
use runner_q::WorkerEngine;
use runner_q_redis::RedisBackend;
use std::sync::Arc;

let backend = RedisBackend::builder()
    .redis_url("redis://localhost:6379")
    .queue_name("my_app")
    .build()
    .await?;  // Returns Result<RedisBackend, runner_q::storage::StorageError>

let engine = WorkerEngine::builder()
    .backend(Arc::new(backend))
    .queue_name("my_app")
    .max_workers(8)
    .build()
    .await?;
```

## License

MIT
