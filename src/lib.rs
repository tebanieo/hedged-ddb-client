use rusoto_core::{ RusotoError, Region };
use rusoto_dynamodb::{ DynamoDb, DynamoDbClient };
use std::time::{ Duration, Instant };
use std::fmt::Debug;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::mpsc;
use log::{ info, error };
use uuid::Uuid;
use chrono::Utc;
use serde_json::json;
use std::sync::atomic::{ AtomicBool, AtomicU64, Ordering };

#[async_trait]
pub trait DynamoOperation {
    type Output;
    type Error: Debug + Send + 'static;

    async fn execute(
        &self,
        client: &DynamoDbClient
    ) -> Result<Self::Output, RusotoError<Self::Error>>;
}

pub struct HedgedDynamoClient {
    client: Arc<DynamoDbClient>,
    hedging_delay: Arc<AtomicU64>,
}

pub struct HedgedRequestResult<T> {
    pub result: T,
    pub winner_request_id: usize,
    pub second_request_sent: bool,
}

impl HedgedDynamoClient {
    pub fn new(region: Region, hedging_delay: Duration) -> Self {
        HedgedDynamoClient {
            client: Arc::new(DynamoDbClient::new(region)),
            hedging_delay: Arc::new(AtomicU64::new(hedging_delay.as_millis() as u64)),
        }
    }

    pub fn update_hedging_delay(&self, new_delay: Duration) {
        let old_delay = self.hedging_delay.swap(new_delay.as_millis() as u64, Ordering::SeqCst);
        info!("Hedging delay updated from {} ms to {} ms", old_delay, new_delay.as_millis());
    }

    pub async fn hedged_request<O>(
        &self,
        operation: O
    )
        -> Result<HedgedRequestResult<O::Output>, RusotoError<O::Error>>
        where
            O: DynamoOperation + Clone + Send + Sync + 'static,
            O::Output: Send + 'static,
            O::Error: Debug + Send + 'static
    {
        let request_id = Uuid::new_v4();
        let start_time = Instant::now();
        let second_request_sent = Arc::new(AtomicBool::new(false));
        let first_request_completed = Arc::new(AtomicBool::new(false));

        info!(
            "{}",
            json!({
            "event": "request_start",
            "hedged_request_id": request_id.to_string(),
            "timestamp": Utc::now().to_rfc3339()
        })
        );

        let (tx, mut rx) = mpsc::channel(2);

        // Spawn first request
        let tx1 = tx.clone();
        let operation1 = operation.clone();
        let client1 = self.client.clone();
        let first_completed = first_request_completed.clone();
        tokio::spawn(async move {
            let request_start = Instant::now();
            let result = operation1.execute(&client1).await;
            first_completed.store(true, Ordering::SeqCst);
            let _ = tx1.send((0, result, request_start.elapsed())).await;
        });

        // Wait for hedging delay using precise_sleep
        let hedging_delay = self.hedging_delay.clone();
        let actual_delay = tokio::task
            ::spawn_blocking(move || {
                let sleep_start = Instant::now();
                let delay = Duration::from_millis(hedging_delay.load(Ordering::SeqCst));
                precise_sleep(delay);
                sleep_start.elapsed()
            }).await
            .unwrap();

        // Check if first request has completed
        if !first_request_completed.load(Ordering::SeqCst) {
            // Spawn second request
            second_request_sent.store(true, Ordering::SeqCst);
            let tx2 = tx;
            let operation2 = operation;
            let client2 = self.client.clone();
            tokio::spawn(async move {
                let request_start = Instant::now();
                let result = operation2.execute(&client2).await;
                let _ = tx2.send((1, result, request_start.elapsed())).await;
            });

            info!(
                "{}",
                json!({
                "event": "hedged_delay",
                "hedged_request_id": request_id.to_string(),
                "actual_delay_ms": actual_delay.as_secs_f64() * 1000.0,
                "timestamp": Utc::now().to_rfc3339()
            })
            );
        }

        let (winner, result, duration) = rx.recv().await.expect("Both tasks have panicked");
        let end_time = Instant::now();

        match &result {
            Ok(_) => {
                info!(
                    "{}",
                    json!({
                    "event": "request_winner",
                    "hedged_request_id": request_id.to_string(),
                    "winner_request_id": winner,
                    "duration_ms": duration.as_secs_f64() * 1000.0,
                    "total_duration_ms": end_time.duration_since(start_time).as_secs_f64() * 1000.0,
                    "timestamp": Utc::now().to_rfc3339()
                })
                );
            }
            Err(e) => {
                error!(
                    "{}",
                    json!({
                    "event": "request_error",
                    "hedged_request_id": request_id.to_string(),
                    "error": format!("{:?}", e),
                    "timestamp": Utc::now().to_rfc3339()
                })
                );
            }
        }

        result.map(|r| HedgedRequestResult {
            result: r,
            winner_request_id: winner,
            second_request_sent: second_request_sent.load(Ordering::SeqCst),
        })
    }
}

#[async_trait]
impl DynamoOperation for rusoto_dynamodb::GetItemInput {
    type Output = rusoto_dynamodb::GetItemOutput;
    type Error = rusoto_dynamodb::GetItemError;

    async fn execute(
        &self,
        client: &DynamoDbClient
    ) -> Result<Self::Output, RusotoError<Self::Error>> {
        client.get_item(self.clone()).await
    }
}

#[async_trait]
impl DynamoOperation for rusoto_dynamodb::QueryInput {
    type Output = rusoto_dynamodb::QueryOutput;
    type Error = rusoto_dynamodb::QueryError;

    async fn execute(
        &self,
        client: &DynamoDbClient
    ) -> Result<Self::Output, RusotoError<Self::Error>> {
        client.query(self.clone()).await
    }
}

#[async_trait]
impl DynamoOperation for rusoto_dynamodb::BatchGetItemInput {
    type Output = rusoto_dynamodb::BatchGetItemOutput;
    type Error = rusoto_dynamodb::BatchGetItemError;

    async fn execute(
        &self,
        client: &DynamoDbClient
    ) -> Result<Self::Output, RusotoError<Self::Error>> {
        client.batch_get_item(self.clone()).await
    }
}

fn precise_sleep(duration: Duration) {
    let start = Instant::now();
    while start.elapsed() < duration {
        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod tests {
    // Add your tests here
}
