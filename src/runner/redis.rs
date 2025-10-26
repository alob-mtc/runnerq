use bb8_redis::RedisConnectionManager;
use std::time::Duration;
use bb8_redis::bb8::{Pool, PooledConnection};
use tokio::time::sleep;
use crate::runner::error::WorkerError;

/// Configuration for Redis connection pool
#[derive(Debug, Clone, Copy)]
pub struct RedisConfig {
    pub max_size: u32,
    pub min_idle: u32,
    pub conn_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            max_size: 50,
            min_idle: 5,
            conn_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(1800),
        }
    }
}

/// Build a pool and verify it with a PING (with retry/backoff).
pub async fn create_redis_pool(redis_url: &str) -> Result<Pool<RedisConnectionManager>, WorkerError> {
    create_redis_pool_with_config(redis_url, RedisConfig::default()).await
}

/// Build a pool with custom configuration and verify it with a PING (with retry/backoff).
pub async fn create_redis_pool_with_config(
    redis_url: &str, 
    config: RedisConfig
) -> Result<Pool<RedisConnectionManager>, WorkerError> {
    tracing::info!(
        "Redis pool: max_size={}, min_idle={}, timeouts: conn={}s idle={}s life={}s",
        config.max_size,
        config.min_idle,
        config.conn_timeout.as_secs(),
        config.idle_timeout.as_secs(),
        config.max_lifetime.as_secs()
    );

    let manager = RedisConnectionManager::new(redis_url)
        .map_err(|e| WorkerError::RedisError(format!("invalid redis url: {} - {}", redacted(redis_url), e)))?;

    let min_idle = config.min_idle.max(1).min(config.max_size);
    if config.max_size == 0 {
        return Err(WorkerError::RedisError("max_size must be > 0".into()));
    }
    let pool = Pool::builder()
        .max_size(config.max_size)
        .min_idle(Some(min_idle))
        .connection_timeout(config.conn_timeout)
        .idle_timeout(Some(config.idle_timeout))
        .max_lifetime(Some(config.max_lifetime))
        .build(manager)
        .await
        .map_err(|e| WorkerError::RedisError(format!("failed to build Redis pool: {}", e)))?;

    // Warm/verify the pool once with retry + exponential backoff
    retry_async(3, Duration::from_millis(400), || async {
        let mut conn = pool.get().await.map_err(|e| WorkerError::RedisError(format!("get() from pool: {}", e)))?;
        redis_ping(&mut conn).await.map_err(|e| WorkerError::RedisError(format!("PING failed: {}", e)))?;
        Ok::<_, WorkerError>(())
    })
    .await
    .map_err(|e| WorkerError::RedisError(format!("unable to verify Redis connectivity after retries: {}", e)))?;

    Ok(pool)
}

/// Get a healthy connection with retry/backoff (if you want to use it at call-sites).
pub async fn get_redis_connection_with_retry<'a>(
    pool: &'a Pool<RedisConnectionManager>,
    max_retries: u32,
) -> Result<PooledConnection<'a, RedisConnectionManager>, WorkerError> {
    retry_async(max_retries, Duration::from_millis(300), || async {
        let mut conn = pool.get().await.map_err(|e| WorkerError::RedisError(format!("get() from pool: {}", e)))?;
        redis_ping(&mut conn).await.map_err(|e| WorkerError::RedisError(format!("PING failed: {}", e)))?;
        Ok::<_, WorkerError>(conn)
    })
    .await
    .map_err(|e| WorkerError::RedisError(format!("failed to get Redis connection after retries: {}", e)))
}

/// Simple PING utility
async fn redis_ping(conn: &mut PooledConnection<'_, RedisConnectionManager>) -> Result<(), WorkerError> {
    let _: String = redis::cmd("PING").query_async(&mut **conn).await
        .map_err(|e| WorkerError::RedisError(format!("Redis PING failed: {}", e)))?;
    Ok(())
}

/// Generic async retry with exponential backoff (jitter optional).
async fn retry_async<F, Fut, T>(max_retries: u32, base_delay: Duration, mut f: F) -> Result<T, WorkerError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, WorkerError>>,
{
    let mut attempt = 0;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_retries => {
                attempt += 1;
                let delay = base_delay.mul_f32(2f32.powi((attempt - 1) as i32));
                tracing::warn!(
                    "retry {}/{} after error: {e:#}. sleeping {:?}",
                    attempt,
                    max_retries,
                    delay
                );
                sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Redact credentials in logs
fn redacted(url: &str) -> String {
    // very light redaction for URIs like: redis://:password@host:6379/db
    if let Some(idx) = url.find('@') {
        let head = &url[..idx];
        if let Some(scheme_end) = head.find("://") {
            let scheme_end = scheme_end + 3;
            return format!("{}***:***{}", &url[..scheme_end], &url[idx..]);
        }
    }
    url.to_string()
}
