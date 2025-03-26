use aws_sdk_dynamodb::Client;
use aws_types::SdkConfig;
use std::time::{Duration, Instant};
use std::fmt::Debug;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::mpsc;
use log::{info, error};
use uuid::Uuid;
use chrono::Utc;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::time::{sleep, Duration as TokioDuration};

#[async_trait]
pub trait DynamoOperation {
    type Output;
    type Error: Debug + Send + 'static;

    async fn execute(&self, client: &Client) -> Result<Self::Output, Self::Error>;
}

pub struct HedgedDynamoClient {
    client: Arc<Client>,
    hedging_delay: Arc<AtomicU64>, //store as microseconds
    is_hedging_enabled: bool,
}

pub struct HedgedRequestResult<T> {
    pub result: T,
    pub winner_request_id: usize,
    pub second_request_sent: bool,
}

impl HedgedDynamoClient {
    pub fn new(config: &SdkConfig, hedging_delay: Duration, is_hedging_enabled: bool) -> Self {
        HedgedDynamoClient {
            client: Arc::new(Client::new(config)),
            hedging_delay: Arc::new(AtomicU64::new(hedging_delay.as_micros() as u64)),
            is_hedging_enabled,
        }
    }

    pub fn update_hedging_delay(&self, new_delay: Duration) {
        let old_delay = self.hedging_delay.swap(new_delay.as_micros() as u64, Ordering::SeqCst);
        info!("Hedging delay updated from {:.2} ms to {:.2} ms", 
            old_delay as f64 / 1000.0, 
            new_delay.as_secs_f64() * 1000.0
        );
    }

    pub fn get_hedging_delay(&self) -> Duration {
        Duration::from_micros(self.hedging_delay.load(Ordering::SeqCst))
    }
    pub async fn hedged_request<O>(
        &self,
        operation: O
    ) -> Result<HedgedRequestResult<O::Output>, O::Error>
    where
        O: DynamoOperation + Clone + Send + Sync + 'static,
        O::Output: Send + 'static,
        O::Error: Debug + Send + 'static
    {
        let request_id = Uuid::new_v4();
        let start_time = Instant::now();
        let second_request_sent = Arc::new(AtomicBool::new(false));
        let first_request_completed = Arc::new(AtomicBool::new(false));
    
        info!("{}", json!({
            "event": "request_start",
            "hedged_request_id": request_id.to_string(),
            "is_hedging_enabled": self.is_hedging_enabled,
            "timestamp": Utc::now().to_rfc3339()
        }));
    
        let (tx, mut rx) = mpsc::channel(2);
    
        // Spawn first request
        let tx1 = tx.clone();
        let operation1 = operation.clone();
        let client1 = self.client.clone();
        let first_completed = first_request_completed.clone();
        tokio::spawn(async move {
            let request_start = Instant::now();
            let result = operation1.execute(&client1).await;
            first_completed.store(true, Ordering::Release);
            let _ = tx1.send((0, result, request_start.elapsed())).await;
        });
    
        let result = if self.is_hedging_enabled {
            let hedging_delay = Duration::from_micros(self.hedging_delay.load(Ordering::Relaxed));
            let delay_future = sleep(TokioDuration::from_micros(hedging_delay.as_micros() as u64));
            tokio::pin!(delay_future);
    
            tokio::select! {
                Some(res) = rx.recv() => res,
                _ = &mut delay_future => {
                    if !first_request_completed.load(Ordering::Acquire) {
                        // Spawn second request
                        second_request_sent.store(true, Ordering::Release);
                        let tx2 = tx;
                        let operation2 = operation;
                        let client2 = self.client.clone();
                        tokio::spawn(async move {
                            let request_start = Instant::now();
                            let result = operation2.execute(&client2).await;
                            let _ = tx2.send((1, result, request_start.elapsed())).await;
                        });
    
                        info!("{}", json!({
                            "event": "hedged_delay",
                            "hedged_request_id": request_id.to_string(),
                            "actual_delay_ms": hedging_delay.as_secs_f64() * 1000.0,
                            "is_hedging_enabled": self.is_hedging_enabled,
                            "timestamp": Utc::now().to_rfc3339()
                        }));
    
                        rx.recv().await.expect("Both tasks have panicked")
                    } else {
                        rx.recv().await.expect("First task has panicked")
                    }
                }
            }
        } else {
            // If hedging is disabled, just wait for the first request to complete
            rx.recv().await.expect("First task has panicked")
        };
    
        let (winner, result, duration) = result;
        let end_time = Instant::now();
    
        match &result {
            Ok(_) => {
                info!("{}", json!({
                    "event": "request_winner",
                    "hedged_request_id": request_id.to_string(),
                    "winner_request_id": winner,
                    "duration_ms": duration.as_secs_f64() * 1000.0,
                    "total_duration_ms": end_time.duration_since(start_time).as_secs_f64() * 1000.0,
                    "is_hedging_enabled": self.is_hedging_enabled,
                    "timestamp": Utc::now().to_rfc3339()
                }));
            }
            Err(e) => {
                error!("{}", json!({
                    "event": "request_error",
                    "hedged_request_id": request_id.to_string(),
                    "error": format!("{:?}", e),
                    "is_hedging_enabled": self.is_hedging_enabled,
                    "timestamp": Utc::now().to_rfc3339()
                }));
            }
        }
    
        result.map(|r| HedgedRequestResult {
            result: r,
            winner_request_id: winner,
            second_request_sent: second_request_sent.load(Ordering::Acquire),
        })
    }
}

// This might not be required anymore.
fn precise_sleep(duration: Duration) {
    let start = Instant::now();
    while start.elapsed() < duration {
        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod tests {
    // Add tests here
}
