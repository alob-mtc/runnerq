//! Redis connection pool management.

use bb8_redis::bb8::Pool;
use bb8_redis::RedisConnectionManager;
use std::time::Duration;
use tokio::time::sleep;

use crate::storage::StorageError;

/// Configuration for Redis connection pool.
#[derive(Debug, Clone, Copy)]
pub struct RedisConfig {
    /// Maximum number of connections in the pool
    pub max_size: u32,
    /// Minimum number of idle connections
    pub min_idle: u32,
    /// Connection timeout
    pub conn_timeout: Duration,
    /// Idle connection timeout
    pub idle_timeout: Duration,
    /// Maximum connection lifetime
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

/// Build a pool with default configuration and verify it with a PING.
#[allow(dead_code)]
pub async fn create_redis_pool(
    redis_url: &str,
) -> Result<Pool<RedisConnectionManager>, StorageError> {
    create_redis_pool_with_config(redis_url, RedisConfig::default()).await
}

/// Build a pool with custom configuration and verify it with a PING.
pub async fn create_redis_pool_with_config(
    redis_url: &str,
    config: RedisConfig,
) -> Result<Pool<RedisConnectionManager>, StorageError> {
    tracing::info!(
        "Redis pool: max_size={}, min_idle={}, timeouts: conn={}s idle={}s life={}s",
        config.max_size,
        config.min_idle,
        config.conn_timeout.as_secs(),
        config.idle_timeout.as_secs(),
        config.max_lifetime.as_secs()
    );

    let manager = RedisConnectionManager::new(redis_url).map_err(|e| {
        StorageError::Configuration(format!(
            "invalid redis url: {} - {}",
            redacted(redis_url),
            e
        ))
    })?;

    let min_idle = config.min_idle.max(1).min(config.max_size);
    if config.max_size == 0 {
        return Err(StorageError::Configuration("max_size must be > 0".into()));
    }

    let pool = Pool::builder()
        .max_size(config.max_size)
        .min_idle(Some(min_idle))
        .connection_timeout(config.conn_timeout)
        .idle_timeout(Some(config.idle_timeout))
        .max_lifetime(Some(config.max_lifetime))
        .build(manager)
        .await
        .map_err(|e| StorageError::Unavailable(format!("failed to build Redis pool: {}", e)))?;

    // Warm/verify the pool once with retry + exponential backoff
    retry_async(3, Duration::from_millis(400), || async {
        let mut conn = pool
            .get()
            .await
            .map_err(|e| StorageError::Unavailable(format!("get() from pool: {}", e)))?;
        redis_ping(&mut conn).await?;
        Ok::<_, StorageError>(())
    })
    .await
    .map_err(|e| {
        StorageError::Unavailable(format!(
            "unable to verify Redis connectivity after retries: {}",
            e
        ))
    })?;

    Ok(pool)
}

/// Simple PING utility
async fn redis_ping(
    conn: &mut bb8_redis::bb8::PooledConnection<'_, RedisConnectionManager>,
) -> Result<(), StorageError> {
    let _: String = redis::cmd("PING")
        .query_async(&mut **conn)
        .await
        .map_err(|e| StorageError::Unavailable(format!("Redis PING failed: {}", e)))?;
    Ok(())
}

/// Generic async retry with exponential backoff.
async fn retry_async<F, Fut, T>(
    max_retries: u32,
    base_delay: Duration,
    mut f: F,
) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StorageError>>,
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
    if let Some(idx) = url.find('@') {
        let head = &url[..idx];
        if let Some(scheme_end) = head.find("://") {
            let scheme_end = scheme_end + 3;
            return format!("{}***:***{}", &url[..scheme_end], &url[idx..]);
        }
    }
    url.to_string()
}
