
use crate::hedged_client::{HedgedDynamoClient, HedgedRequestResult};
use crate::dynamodb_operations::GetItemOperation;
use crate::metrics::{Metrics, TestRunResult};

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::operation::get_item::GetItemOutput;

use std::collections::HashMap;
use std::time::{Duration, Instant};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use futures::future::join_all;
use std::error::Error;
use std::sync::Arc;
use log::{info};
use chrono::Utc;
use serde_json::{json, Value};
use rand::seq::SliceRandom;


const MAX_RPS: usize = 1;
const WARM_UP_BATCH_SIZE: usize = 1;
const RAMP_UP_INITIAL_BATCH_SIZE: usize = 1;
const RAMP_UP_STEPS: usize = 1;

pub struct LoadTestConfig {
    pub warm_up_duration: Duration,
    pub ramp_up_duration: Duration,
    pub main_test_duration: Duration,
    pub update_hedging_duration: Duration,
    pub percentile: f64,
    pub test_id: String,
    pub test_items: Vec<Value>,
}

async fn warm_up_phase(client: &Arc<HedgedDynamoClient>, table_name: &str, config: &LoadTestConfig) -> Result<(), Box<dyn Error>> {
    
    info!("{}", json!({
        "event": "warm-up-start",
        "test_id": config.test_id,
        "duration": config.warm_up_duration.as_secs(),
        "initial_hedging_delay": client.get_hedging_delay().as_millis(),
        "timestamp": Utc::now().to_rfc3339()
    }));

    let start_time = Instant::now();

    println!("Starting warm-up phase for {:?}", config.warm_up_duration);

    let pb = ProgressBar::new(config.warm_up_duration.as_secs());
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} Warm-up: [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    while start_time.elapsed() < config.warm_up_duration {
        let _ = send_batch_requests(client, table_name, WARM_UP_BATCH_SIZE, &config.test_items).await;
        pb.set_position(start_time.elapsed().as_secs());
    }

    pb.finish_with_message("Warm-up completed");
    info!("{}", json!({
        "event": "warm-up-completed",
        "test_id": config.test_id,
        "duration": config.warm_up_duration.as_secs(),
        "initial_hedging_delay": client.get_hedging_delay().as_millis(),
        "timestamp": Utc::now().to_rfc3339()
    }));
    println!("Warm-up phase completed");
    Ok(())
}

async fn ramp_up_phase(client: &Arc<HedgedDynamoClient>, table_name: &str, config: &LoadTestConfig) -> Result<Metrics, Box<dyn Error>> {
    
    info!("{}", json!({
        "event": "ramp-up-start",
        "test_id": config.test_id,
        "duration": config.ramp_up_duration.as_secs(),
        "initial_hedging_delay": client.get_hedging_delay().as_millis(),
        "timestamp": Utc::now().to_rfc3339()
    }));

    let start_time = Instant::now();
    let mut metrics = Metrics::new();
    let batch_size_increment = (MAX_RPS - RAMP_UP_INITIAL_BATCH_SIZE) / RAMP_UP_STEPS;
    let step_duration = config.ramp_up_duration / RAMP_UP_STEPS as u32;

    println!("Starting ramp-up phase for {:?}", config.ramp_up_duration);

    let pb = ProgressBar::new(config.ramp_up_duration.as_secs());
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} Ramp-up: [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("#>-"));

    for step in 0..RAMP_UP_STEPS {
        let batch_size = RAMP_UP_INITIAL_BATCH_SIZE + (step * batch_size_increment);
        let step_start = Instant::now();

        while step_start.elapsed() < step_duration {
            let batch_results = send_batch_requests(client, table_name, batch_size, &config.test_items).await;
            update_metrics(&mut metrics, &batch_results);
            pb.set_position(start_time.elapsed().as_secs());
            pb.set_message(format!("Batch size: {}", batch_size));
        }
    }

    pb.finish_with_message("Ramp-up completed");
    println!("Ramp-up phase completed");

    info!("{}", json!({
        "event": "ramp-up-completed",
        "test_id": config.test_id,
        "total_duration_ms": start_time.elapsed().as_millis(),
        "total_operations": metrics.total_operations,
        "timestamp": Utc::now().to_rfc3339()
    }));

    Ok(metrics)
}



pub async fn main_load_test(client: &Arc<HedgedDynamoClient>, table_name: &str, config: &LoadTestConfig) -> Result<Metrics, Box<dyn Error>> {
    info!("{}", json!({
        "event": "main_test_start",
        "test_id": config.test_id,
        "duration": config.main_test_duration.as_secs(),
        "initial_hedging_delay": client.get_hedging_delay().as_millis(),
        "timestamp": Utc::now().to_rfc3339()
    }));
    
    let start_time = Instant::now();
    let metrics = Arc::new(tokio::sync::Mutex::new(Metrics::new()));
    
    let mut last_update = Instant::now();

    let current_hedging_delay = client.get_hedging_delay();

    if config.percentile > 0.0 {
        println!("Initial hedging delay for main test: {:.2} ms", current_hedging_delay.as_secs_f64() * 1000.0);
    } else {
        println!("Hedging is disabled for this test");
    }

    let multi = MultiProgress::new();
    let pb = multi.add(ProgressBar::new(config.main_test_duration.as_secs()));
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let stats_pb = multi.add(ProgressBar::new(1));
    stats_pb.set_style(ProgressStyle::default_bar().template("{msg}").unwrap());

    let stats_display = Arc::new(stats_pb);

    // Spawn a task to update the statistics display
    let update_stats = {
        let stats_display = Arc::clone(&stats_display);
        let metrics = Arc::clone(&metrics);
        let client = Arc::clone(client);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let metrics = metrics.lock().await;
                let elapsed = start_time.elapsed();
                let ops_per_second = metrics.total_operations as f64 / elapsed.as_secs_f64();
                let hedged_percent = (metrics.second_requests_sent as f64 / metrics.total_operations as f64) * 100.0;
                let win_1_percent = (metrics.first_requests_won as f64 / metrics.total_operations as f64) * 100.0;
                let win_2_percent = (metrics.second_requests_won as f64 / metrics.total_operations as f64) * 100.0;
                let current_delay = client.get_hedging_delay().as_micros();
                let p50 = metrics.calculate_percentile(50.0);
                let p90 = metrics.calculate_percentile(90.0);
                let p99 = metrics.calculate_percentile(99.0);

                let stats = format!(
                    "OPS: {:.1} | Hedged: {:.1}% | Win 1: {:.1}% | Win 2: {:.1}% | Delay: {:.2}ms | P50: {:.2}ms | P90: {:.2} | ms P99: {:.2}ms",
                    ops_per_second, 
                    hedged_percent, 
                    win_1_percent, 
                    win_2_percent, 
                    current_delay as f64 / 1000.0, 
                    p50 as f64 / 1000.0, 
                    p90 as f64 / 1000.0,
                    p99 as f64 / 1000.0
                );
                stats_display.set_message(stats);
            }
        })
    };

    let mut last_progress_update = Instant::now();
    let progress_update_interval = Duration::from_millis(500); // Twice per second

    while start_time.elapsed() < config.main_test_duration {
        let batch_results = send_batch_requests(client, table_name, MAX_RPS, &config.test_items).await;
        {
            let mut metrics = metrics.lock().await;
            update_metrics(&mut metrics, &batch_results);
        }

        if config.percentile > 0.0 && last_update.elapsed() >= config.update_hedging_duration {
            let new_delay = {
                let metrics = metrics.lock().await;
                metrics.calculate_percentile(config.percentile)
            };
            client.update_hedging_delay(Duration::from_micros(new_delay));
            last_update = Instant::now();
        }

        if last_progress_update.elapsed() >= progress_update_interval {
            pb.set_position(start_time.elapsed().as_secs());
            last_progress_update = Instant::now();
        }
    }

    pb.finish_with_message("Load test completed");
    update_stats.abort(); // Stop the stats update task
    println!("Main load test phase completed");

    // Clone the metrics before returning
    let final_metrics = metrics.lock().await.clone();
    info!("{}", json!({
        "event": "main_test_completed",
        "test_id": config.test_id,
        "total_duration_ms": start_time.elapsed().as_millis(),
        "total_operations": final_metrics.total_operations,
        "timestamp": Utc::now().to_rfc3339()
    }));

    let metrics_json = final_metrics.get_final_metrics_json(config.main_test_duration, config.percentile);
    info!("Final Metrics: {}\n", serde_json::to_string_pretty(&metrics_json).unwrap());

    Ok(final_metrics)
}

pub async fn run_full_load_test(
    client: Arc<HedgedDynamoClient>,
    table_name: String,
    config: LoadTestConfig,
    initial_delay: Option<Duration>,
) -> Result<TestRunResult, Box<dyn Error>> {
    let initial_hedging_delay = client.get_hedging_delay();
    info!("{}", json!({
        "event": "load_test_start",
        "test_id": config.test_id,
        "is_hedging_enabled": config.percentile > 0.0,
        "initial_hedging_delay_ms": initial_hedging_delay.as_millis(),
        "timestamp": Utc::now().to_rfc3339()
    }));

    if config.percentile == 0.0 {
        // This is the non-hedged test
        println!("Running non-hedged test");
        warm_up_phase(&client, &table_name, &config).await?;
        ramp_up_phase(&client, &table_name, &config).await?;
    } else {
        // This is a hedged test
        if let Some(delay) = initial_delay {
            client.update_hedging_delay(delay);
            println!("Setting initial hedging delay to {:.2} ms based on non-hedged test results", delay.as_secs_f64() * 1000.0);
        } else {
            println!("No initial delay provided for hedged test");
        }
    }
  
    let final_metrics = main_load_test(&client, &table_name, &config).await?;

    final_metrics.print_final(config.main_test_duration);

    let test_run_result = final_metrics.generate_test_run_result(config.main_test_duration, config.percentile);

    info!("{}", json!({
        "event": "load_test_completed",
        "test_id": config.test_id,
        "is_hedging_enabled": config.percentile > 0.0,
        "final_hedging_delay_ms": client.get_hedging_delay().as_millis(),
        "total_requests": test_run_result.total_requests,
        "successful_requests": test_run_result.successful_requests,
        "failed_requests": test_run_result.failed_requests,
        "requests_per_second": test_run_result.requests_per_second,
        "p99_latency_ms": test_run_result.p99_latency,
        "timestamp": Utc::now().to_rfc3339()
    }));

    Ok(test_run_result)
}

async fn send_batch_requests(
    client: &HedgedDynamoClient, 
    table_name: &str, 
    batch_size: usize,
    test_items: &[Value]
) -> Vec<Result<(HedgedRequestResult<GetItemOutput>, Duration), Box<dyn Error + Send + Sync>>> {
    let futures: Vec<_> = (0..batch_size)
        .map(|_| send_single_request(client, table_name, test_items))
        .collect();

    join_all(futures).await
}

async fn send_single_request(
    client: &HedgedDynamoClient, 
    table_name: &str,
    test_items: &[Value]
) -> Result<(HedgedRequestResult<GetItemOutput>, Duration), Box<dyn Error + Send + Sync>> {

    let item = test_items.choose(&mut rand::thread_rng()).expect("No test items available");

    let mut key = HashMap::new();
    if let Some(pk) = item["PK"]["S"].as_str() {key.insert("PK".to_string(), AttributeValue::S(pk.to_string()));}
    if let Some(sk) = item["SK"]["S"].as_str() {key.insert("SK".to_string(), AttributeValue::S(sk.to_string()));}
    
    let get_item_op = GetItemOperation {
        table_name: table_name.to_string(),
        key,
    };
    let start = Instant::now();
    let result = client.hedged_request(get_item_op).await;
    let duration = start.elapsed();

    result.map(|r| (r, duration)).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}

fn update_metrics(metrics: &mut Metrics, batch_results: &[Result<(HedgedRequestResult<GetItemOutput>, Duration), Box<dyn Error + Send + Sync>>]) {
    metrics.total_operations += batch_results.len();
    
    for result in batch_results {
        match result {
            Ok((hedged_result, duration)) => {
                metrics.total_successes += 1;
                if hedged_result.winner_request_id == 0 {
                    metrics.first_requests_won += 1;
                } else {
                    metrics.second_requests_won += 1;
                }
                if hedged_result.second_request_sent {
                    metrics.second_requests_sent += 1;
                }
                // Record the latency for this individual request
                metrics.histogram.record(duration.as_micros() as u64).unwrap();
            }
            Err(_) => {
                metrics.total_failures += 1;
            }
        }
    }
}
