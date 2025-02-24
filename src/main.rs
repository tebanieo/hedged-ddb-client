mod hedged_client;
mod dynamodb_operations;
mod load_test;
mod metrics;

use crate::hedged_client::HedgedDynamoClient;
use crate::load_test::LoadTestConfig;
use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;
use aws_types::sdk_config::SdkConfig;
use std::sync::Arc;
use std::time::Duration;
use log4rs::{
    append::rolling_file::{
        RollingFileAppender,
        policy::compound::{
            CompoundPolicy,
            trigger::size::SizeTrigger,
            roll::fixed_window::FixedWindowRoller,
        },
    },
    config::{ Appender, Config, Root },
    encode::pattern::PatternEncoder,
};
use log::LevelFilter;
use chrono::Utc;
use std::error::Error;
use std::fs;
use rand::seq::SliceRandom;

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use serde_json::Value;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Instant;
use crate::metrics::TestRunResult;

use std::io::Write;

fn setup_logging(test_id: &str) -> Result<(), Box<dyn Error>> {
    // Create a directory for this test run
    let log_dir = format!("./log/{}", test_id);
    fs::create_dir_all(&log_dir)?;

    // Set up the roller
    let window_size = 5; // Keep 5 archived log files
    let fixed_window_roller = FixedWindowRoller::builder()
        .build(&format!("{}/output-{{}}.log", log_dir), window_size)?;

    // Set up the size-based trigger policy
    let size_limit = 10 * 1024 * 1024; // 10MB
    let size_trigger = SizeTrigger::new(size_limit);

    // Combine the roller and the trigger into a compound policy
    let compound_policy = CompoundPolicy::new(
        Box::new(size_trigger),
        Box::new(fixed_window_roller)
    );

    // Set up the rolling file appender
    let rolling_appender = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}{n}")))
        .build(&format!("{}/output.log", log_dir), Box::new(compound_policy))?;

    // Build the log4rs config
    let config = Config::builder()
        .appender(Appender::builder().build("rolling", Box::new(rolling_appender)))
        .build(Root::builder().appender("rolling").build(LevelFilter::Info))?;

    // Initialize the logger
    log4rs::init_config(config)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let date_test = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    let _ = setup_logging(&date_test);    
    let results_dir = ".";
    fs::create_dir_all(results_dir)?; // Create the directory if it doesn't exist
    let csv_filename = format!("{}/test_results_{}.csv", results_dir, date_test);

    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    let file_path = "sample_data.json";
    let items = load_items_from_file(file_path)?;
    println!("Total items loaded: {}", items.len());

    let test_items = pick_random_items(&items, 10000);

    // Define the percentiles to test
    let percentiles = vec![99.0, 90.0, 75.0, 50.0, 10.0];

    // Number of iterations for the entire test suite
    let num_iterations = 5;

    let mut all_results = Vec::new();
    let mut initial_delay = None;

    for iteration in 1..=num_iterations {
        println!("Starting iteration {} of {}", iteration, num_iterations);

        // Run non-hedged test first
        println!("Running non-hedged test for iteration {}", iteration);
        let non_hedged_result = run_single_test(&config, &test_items, None, iteration, &date_test, None).await?;
        all_results.push(non_hedged_result.clone());

        // Run hedged tests for all percentiles
        for &percentile in &percentiles {
            // Calculate initial delay for hedged tests based on non-hedged results
            initial_delay = Some(Duration::from_micros(non_hedged_result.metrics.calculate_percentile(percentile) as u64));
            println!("Running hedged test for p{} in iteration {}", percentile, iteration);
            let hedged_result = run_single_test(&config, &test_items, Some(percentile), iteration, &date_test, initial_delay).await?;
            all_results.push(hedged_result);
        }

        println!("Completed iteration {}", iteration);
    }

    // Generate CSV with all results
    generate_csv_report(&all_results, &csv_filename)?;

    println!("All load tests completed");
    println!("Test results have been saved to {}", csv_filename);
    Ok(())
}

fn load_items_from_file(file_path: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);

    // Get the total file size
    let file_size = reader.seek(SeekFrom::End(0))?;
    reader.seek(SeekFrom::Start(0))?;

    let pb = ProgressBar::new(file_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let mut items = Vec::new();
    let start_time = Instant::now();

    for line in reader.lines() {
        let line = line?;
        let value: Value = serde_json::from_str(&line)?;
        if let Some(item) = value.get("Item") {
            items.push(item.clone());
        }
        pb.inc(line.len() as u64 + 1); // +1 for the newline character
    }

    pb.finish_with_message("Done");
    let duration = start_time.elapsed();

    println!("Loaded {} items in {:.2?}", items.len(), duration);

    Ok(items)
}

fn pick_random_items(items: &[Value], n: usize) -> Vec<Value> {
    let mut rng = rand::thread_rng();
    items.choose_multiple(&mut rng, n).cloned().collect()
}

async fn run_single_test(
    config: &SdkConfig,
    test_items: &[Value],
    percentile: Option<f64>,
    iteration: usize,
    date_test: &str,
    initial_delay: Option<Duration>,
) -> Result<TestRunResult, Box<dyn std::error::Error>> {
    let is_hedging_enabled = percentile.is_some();
    let client = Arc::new(HedgedDynamoClient::new(
        config,
        if is_hedging_enabled {
            Duration::from_millis(10) // Initial hedging delay when enabled
        } else {
            Duration::from_secs(1) // Large delay when disabled
        },
        is_hedging_enabled
    ));

    let test_type = if is_hedging_enabled { 
        format!("hedged_p{}", percentile.unwrap())
    } else {
        "non_hedged".to_string()
    };

    let load_test_config = LoadTestConfig {
        warm_up_duration: Duration::from_secs(60),
        ramp_up_duration: Duration::from_secs(120),
        main_test_duration: Duration::from_secs(600),
        update_hedging_duration: Duration::from_secs(10),
        test_id: format!("{}_iter{}_{}", date_test, iteration, test_type),
        percentile: percentile.unwrap_or(0.0), // Use 0.0 for non-hedged tests
        test_items: test_items.to_vec(),
    };

    load_test::run_full_load_test(
        client,
        "sample-org-table".to_string(),
        load_test_config,
        initial_delay
    ).await
}

fn generate_csv_report(results: &[TestRunResult], filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(filename)?;
    
    // Write header
    writeln!(file, "Iteration,Test Type,Percentile,Total Requests,Successful Requests,Failed Requests,First Requests Won,Second Requests Won,Second Requests Sent,Requests per Second,Success Rate,Hedging Rate,Second Request Win Rate,P1 Latency (ms),P25 Latency (ms),P50 Latency (ms),P75 Latency (ms),P90 Latency (ms),P95 Latency (ms),P99 Latency (ms),P99.9 Latency (ms)")?;

    // Write data
    for (index, result) in results.iter().enumerate() {
        let iteration = index / 6; // 7 tests per iteration (1 non-hedged + 6 hedged)
        let test_type = if result.percentile == 0.0 { "Non-Hedged" } else { "Hedged" };
        let percentile = format!("{:.1}", result.percentile) ;
        
        writeln!(file, "{},{},{},{},{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
            iteration,
            test_type,
            percentile,
            result.total_requests,
            result.successful_requests,
            result.failed_requests,
            result.first_requests_won,
            result.second_requests_won,
            result.second_requests_sent,
            result.requests_per_second,
            result.success_rate,
            result.hedging_rate,
            result.second_request_win_rate,
            result.p1_latency,
            result.p25_latency,
            result.p50_latency,
            result.p75_latency,
            result.p90_latency,
            result.p95_latency,
            result.p99_latency,
            result.p999_latency
        )?;
    }

    Ok(())
}
