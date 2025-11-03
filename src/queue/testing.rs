
// use super::queue::*;

// #[cfg(test)]
// mod tests {
//     use crate::{activity::activity::{Activity,ActivityStatus}, ActivityPriority};
//     use bb8_redis::{bb8::Pool, RedisConnectionManager};
//     use chrono::Utc;
//     use super::*;

//     #[tokio::test]
//     async fn test_priority_score_calculation() {
//         let (redis_url, redis_container) = setup_redis_test_environment().await;
//         let queue = ActivityQueue::new(
//             // Mock pool - not used in this test
//             Pool::builder()
//                 // .build(bb8_redis::RedisConnectionManager::new("redis://localhost").unwrap())
//                 .build(bb8_redis::RedisConnectionManager::new(redis_url).unwrap())
//                 .await
//                 .unwrap(),
//             "test".to_string(),
//         );

//         let now = Utc::now();

//         // Test priority ordering
//         let critical_score = queue.calculate_priority_score(&ActivityPriority::Critical, now);
//         let high_score = queue.calculate_priority_score(&ActivityPriority::High, now);
//         let normal_score = queue.calculate_priority_score(&ActivityPriority::Normal, now);
//         let low_score = queue.calculate_priority_score(&ActivityPriority::Low, now);

//         assert!(critical_score > high_score);
//         assert!(high_score > normal_score);
//         assert!(normal_score > low_score);

//         // Test FIFO within same priority
//         let later = now + chrono::Duration::microseconds(1000);
//         let earlier_score = queue.calculate_priority_score(&ActivityPriority::Normal, now);
//         let later_score = queue.calculate_priority_score(&ActivityPriority::Normal, later);

//         assert!(
//             later_score > earlier_score,
//             "Later activities should have higher scores for FIFO ordering"
//         );
//     }



    
//     use testcontainers::{core::{IntoContainerPort, WaitFor, ContainerAsync}, runners::AsyncRunner, GenericImage, ImageExt};

//     async fn setup_redis_test_environment() -> (String, ContainerAsync<GenericImage>)  {
//         let port = 4030;
//        let container = GenericImage::new("redis", "7.2.4")
//             .with_exposed_port(port.tcp())
//             .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
//             .with_network("bridge")
//             .with_env_var("DEBUG", "1")
//             .start()
//             .await
//             .expect("Failed to start Redis");
//         (
//             format!("redis://{}:{}",
//                 container.get_host().await.expect("Failed to get host"),
//                 container.get_host_port_ipv4(port).await.expect("Failed to get host port")
//             ),
//             container,
//         )
//     }
// }
