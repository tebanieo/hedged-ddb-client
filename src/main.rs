use ddb_hedged_request::HedgedDynamoClient;
use rusoto_core::Region;
use rusoto_dynamodb::{ AttributeValue, GetItemInput };
use std::{ collections::HashMap, time::{ Instant, Duration }, sync::{ Arc, Mutex } };
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

use futures::future::join_all;
use log::info;
use tokio::time;
use tokio::runtime::Runtime;
use num_cpus;
use std::cmp;
use hdrhistogram::Histogram;
use clap::Parser;
use indicatif::{ ProgressBar, ProgressStyle, MultiProgress };

const DEFAULT_DELAY: u64 = 3000; //3 seconds

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value_t = 4)]
    max_threads: usize,

    #[clap(long, default_value_t = 50)]
    batch_size: usize,

    #[clap(long, default_value_t = 0)]
    delay_between_batches: u64,

    #[clap(long, default_value_t = 10)]
    test_duration: u64,

    #[clap(long, default_value_t = 70.0)]
    percentile: f64,

    #[clap(long, default_value_t = 30)] // 30 seconds
    warmup_duration: u64,
}

#[derive(Clone)]
struct Metrics {
    histogram: Histogram<u64>,
    total_operations: usize,
    total_successes: usize,
    total_failures: usize,
    first_requests_won: usize,
    second_requests_won: usize,
    second_requests_sent: usize,
    start_time: Instant,
}

impl Metrics {
    fn new() -> Self {
        Metrics {
            histogram: Histogram::<u64>::new(3).unwrap(),
            total_operations: 0,
            total_successes: 0,
            total_failures: 0,
            first_requests_won: 0,
            second_requests_won: 0,
            second_requests_sent: 0,
            start_time: Instant::now(),
        }
    }

    fn calculate_new_delay(&self, percentile: f64) -> u64 {
        let value_at_percentile = self.histogram.value_at_quantile(percentile / 100.0);
        ((value_at_percentile as f64) / 1_000.0) as u64 // Convert to millis
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    setup_logging()?;
    let runtime = Runtime::new()?;
    println!("Args: {:?}", args);
    runtime.block_on(async_main(args))
}

fn setup_logging() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the roller
    let window_size = 20; // Keep 3 archived log files
    let fixed_window_roller = FixedWindowRoller::builder().build("log/output-{}.log", window_size)?;

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
        .build("log/output.log", Box::new(compound_policy))?;

    // Build the log4rs config
    let config = Config::builder()
        .appender(Appender::builder().build("rolling", Box::new(rolling_appender)))
        .build(Root::builder().appender("rolling").build(LevelFilter::Info))?;

    // Initialize the logger
    log4rs::init_config(config)?;

    Ok(())
}

fn get_num_threads(max_threads: usize) -> usize {
    cmp::min(max_threads, num_cpus::get())
}

async fn async_main(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let num_threads = get_num_threads(args.max_threads);
    info!("Running with {} threads", num_threads);

    let metrics = Arc::new(Mutex::new(Metrics::new()));

    // Warm-up phase
    info!("Starting warm-up phase for {} seconds", args.warmup_duration);
    let warmup_progress = ProgressBar::new(args.warmup_duration);
    warmup_progress.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} Warm-up: [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} ({eta})"
            )
            .unwrap()
            .progress_chars("#>-")
    );

    let warmup_progress = Arc::new(warmup_progress);

    let warmup_handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let metrics = Arc::clone(&metrics);
            let args = args.clone();
            let progress = Arc::clone(&warmup_progress);
            tokio::spawn(async move {
                run_warmup(thread_id, metrics, &args, progress).await;
            })
        })
        .collect();

    // Spawn a task to keep the progress bar updated
    let warmup_progress_handle = {
        let progress = Arc::clone(&warmup_progress);
        tokio::spawn(async move {
            let start = Instant::now();
            while start.elapsed().as_secs() < args.warmup_duration {
                progress.set_position(start.elapsed().as_secs());
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    };

    for handle in warmup_handles {
        handle.await?;
    }

    // Wait for the progress bar update task to finish
    warmup_progress_handle.await?;

    warmup_progress.finish_with_message("Warm-up completed");

    // Calculate the new hedging delay based on warm-up metrics
    let initial_hedging_delay = {
        let warmup_metrics = metrics.lock().unwrap();
        warmup_metrics.calculate_new_delay(args.percentile)
    };

    info!("Initial hedging delay after warm-up: {} ms", initial_hedging_delay);

    // Reset metrics for the actual test
    let warmup_metrics = metrics.lock().unwrap().clone();
    *metrics.lock().unwrap() = Metrics::new();

    // Actual load test
    info!("Starting load test for {} seconds", args.test_duration);
    let start_time = Instant::now();

    let multi_progress = Arc::new(MultiProgress::new());
    let main_progress = multi_progress.add(ProgressBar::new(args.test_duration as u64));
    main_progress.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})"
            )
            .unwrap()
            .progress_chars("#>-")
    );

    let stats_progress = multi_progress.add(ProgressBar::new(1));
    stats_progress.set_style(ProgressStyle::default_bar().template("{wide_msg}").unwrap());

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let metrics = Arc::clone(&metrics);
            let args = args.clone();
            let main_pb = main_progress.clone();
            let stats_pb = stats_progress.clone();
            tokio::spawn(async move {
                run_load_test(
                    thread_id,
                    metrics,
                    &args,
                    main_pb,
                    stats_pb,
                    initial_hedging_delay
                ).await;
            })
        })
        .collect();

    let update_progress = {
        let metrics = Arc::clone(&metrics);
        let main_progress = main_progress.clone();
        let stats_progress = stats_progress.clone();
        let percentile = args.percentile;
        let test_duration = args.test_duration;
        tokio::spawn(async move {
            let start = Instant::now();
            while start.elapsed().as_secs() < test_duration {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let elapsed = start.elapsed().as_secs();
                main_progress.set_position(elapsed);
                update_stats_display(&metrics, percentile, &stats_progress);
            }
        })
    };

    // Wait for all load test threads to complete
    for handle in handles {
        handle.await?;
    }

    // Wait for the progress update task to complete
    update_progress.await?;

    main_progress.finish_with_message("Load test completed");

    let total_duration = start_time.elapsed();
    let metrics = metrics.lock().unwrap();
    log_results(&metrics, &warmup_metrics, total_duration, args.percentile);

    Ok(())
}

async fn run_warmup(
    thread_id: usize,
    metrics: Arc<Mutex<Metrics>>,
    args: &Args,
    progress: Arc<ProgressBar>
) {
    let client = Arc::new(
        HedgedDynamoClient::new(Region::UsEast1, Duration::from_millis(DEFAULT_DELAY))
    );
    let table_name = "describe_sample_table".to_string();

    let start_time = Instant::now();
    let warmup_duration = Duration::from_secs(args.warmup_duration);

    while start_time.elapsed() < warmup_duration {
        let batch_start = Instant::now();
        let (successes, failures, first_won, second_won, second_sent) = send_batch(
            &client,
            &table_name,
            args.batch_size
        ).await;
        let batch_duration = batch_start.elapsed();

        update_metrics(
            metrics.clone(),
            args.batch_size,
            successes,
            failures,
            first_won,
            second_won,
            second_sent,
            batch_duration,
            thread_id
        );

        if thread_id == 0 {
            progress.clone().set_position(start_time.elapsed().as_secs());
        }

        if start_time.elapsed().as_secs() % 5 == 0 {
            let new_delay = {
                let metrics = metrics.lock().unwrap();
                metrics.calculate_new_delay(args.percentile)
            };
            client.update_hedging_delay(Duration::from_millis(new_delay));
            info!(
                "Warm-up: Updated hedging delay to {} ms - {}th percentile",
                new_delay,
                args.percentile
            );
        }

        if
            start_time.elapsed() + Duration::from_millis(args.delay_between_batches) >
            warmup_duration
        {
            break;
        }

        time::sleep(Duration::from_millis(args.delay_between_batches)).await;
    }
}

async fn run_load_test(
    thread_id: usize,
    metrics: Arc<Mutex<Metrics>>,
    args: &Args,
    main_progress: ProgressBar,
    stats_progress: ProgressBar,
    initial_hedging_delay: u64
) {
    let client = Arc::new(
        HedgedDynamoClient::new(Region::UsEast1, Duration::from_millis(initial_hedging_delay))
    );
    let table_name = "describe_sample_table".to_string();

    let start_time = Instant::now();
    let test_duration = Duration::from_secs(args.test_duration);

    while start_time.elapsed() < test_duration {
        let batch_start = Instant::now();
        let (successes, failures, first_won, second_won, second_sent) = send_batch(
            &client,
            &table_name,
            args.batch_size
        ).await;
        let batch_duration = batch_start.elapsed();

        update_metrics(
            metrics.clone(),
            args.batch_size,
            successes,
            failures,
            first_won,
            second_won,
            second_sent,
            batch_duration,
            thread_id
        );

        if thread_id == 0 {
            // Update main progress bar
            main_progress.set_position(start_time.elapsed().as_secs());

            // Update stats display
            update_stats_display(&metrics, args.percentile, &stats_progress);
        }

        if start_time.elapsed().as_secs() % 5 == 0 {
            let new_delay = {
                let metrics = metrics.lock().unwrap();
                metrics.calculate_new_delay(args.percentile)
            };
            client.update_hedging_delay(Duration::from_millis(new_delay));
            info!("Updated hedging delay to {} ms - {}th percentile", new_delay, args.percentile);
        }

        if start_time.elapsed() + Duration::from_millis(args.delay_between_batches) > test_duration {
            break;
        }

        time::sleep(Duration::from_millis(args.delay_between_batches)).await;
    }
}

async fn send_batch(
    client: &HedgedDynamoClient,
    table_name: &str,
    batch_size: usize
) -> (usize, usize, usize, usize, usize) {
    let requests: Vec<_> = (0..batch_size)
        .map(|_| {
            let mut key = HashMap::new();
            key.insert("PK".to_string(), AttributeValue {
                s: Some("bee".to_string()),
                ..Default::default()
            });
            key.insert("SK".to_string(), AttributeValue {
                s: Some("bee".to_string()),
                ..Default::default()
            });

            let input = GetItemInput {
                table_name: table_name.to_string(),
                key,
                ..Default::default()
            };

            client.hedged_request(input)
        })
        .collect();

    let results = join_all(requests).await;

    results
        .into_iter()
        .fold((0, 0, 0, 0, 0), |(successes, failures, first_won, second_won, second_sent), result| {
            match result {
                Ok(hedged_result) => {
                    let first_won_inc = if hedged_result.winner_request_id == 0 { 1 } else { 0 };
                    let second_won_inc = if hedged_result.winner_request_id != 0 { 1 } else { 0 };
                    let second_sent_inc = if hedged_result.second_request_sent { 1 } else { 0 };
                    (
                        successes + 1,
                        failures,
                        first_won + first_won_inc,
                        second_won + second_won_inc,
                        second_sent + second_sent_inc,
                    )
                }
                Err(_) => (successes, failures + 1, first_won, second_won, second_sent),
            }
        })
}

fn update_metrics(
    metrics: Arc<Mutex<Metrics>>,
    batch_size: usize,
    successes: usize,
    failures: usize,
    first_won: usize,
    second_won: usize,
    second_sent: usize,
    batch_duration: Duration,
    thread_id: usize
) {
    let mut metrics = metrics.lock().unwrap();
    metrics.total_operations += batch_size;
    metrics.total_successes += successes;
    metrics.total_failures += failures;
    metrics.first_requests_won += first_won;
    metrics.second_requests_won += second_won;
    metrics.second_requests_sent += second_sent;
    metrics.histogram.record(batch_duration.as_micros() as u64).unwrap();

    if metrics.total_operations % 1000 == 0 {
        info!(
            "Thread {}: Total ops: {}, Successes: {}, Failures: {}, First won: {}, Second won: {}, Second sent: {}",
            thread_id,
            metrics.total_operations,
            metrics.total_successes,
            metrics.total_failures,
            metrics.first_requests_won,
            metrics.second_requests_won,
            metrics.second_requests_sent
        );
    }
}

fn update_stats_display(metrics: &Arc<Mutex<Metrics>>, percentile: f64, progress: &ProgressBar) {
    let metrics = metrics.lock().unwrap();
    let total_duration = metrics.start_time.elapsed().as_secs_f64();
    let ops_per_second = (metrics.total_operations as f64) / total_duration;
    let stats = format!(
        "Ops: {} ({:.2}/s) | Succ: {} | Fail: {} | 1st: {} | 2nd: {} | 2nd Sent: {} | {}th ile: {:.2}ms | 95th ile: {:.2}ms | 99th ile: {:.2}ms",
        metrics.total_operations,
        ops_per_second,
        metrics.total_successes,
        metrics.total_failures,
        metrics.first_requests_won,
        metrics.second_requests_won,
        metrics.second_requests_sent,
        percentile,
        metrics.calculate_new_delay(percentile),
        (metrics.histogram.value_at_quantile(0.95) as f64) / 1_000.0,
        (metrics.histogram.value_at_quantile(0.99) as f64) / 1_000.0
    );
    progress.set_message(stats);
}

fn log_results(
    metrics: &Metrics,
    warmup_metrics: &Metrics,
    total_duration: Duration,
    percentile: f64
) {
    info!("All tests completed. Total time (including warm-up): {:?}", total_duration);
    info!("Warm-up metrics:");
    log_metrics(warmup_metrics, "Warm-up");
    info!("Test metrics:");
    log_metrics(metrics, "Test");
    info!(
        "Average operations per second: {:.2}",
        (metrics.total_operations as f64) / total_duration.as_secs_f64()
    );

    info!("Latency percentiles (milliseconds):");
    info!("50th percentile: {:.2}", (metrics.histogram.value_at_quantile(0.5) as f64) / 1_000.0);
    info!("90th percentile: {:.2}", (metrics.histogram.value_at_quantile(0.9) as f64) / 1_000.0);
    info!("95th percentile: {:.2}", (metrics.histogram.value_at_quantile(0.95) as f64) / 1_000.0);
    info!("99th percentile: {:.2}", (metrics.histogram.value_at_quantile(0.99) as f64) / 1_000.0);
    info!(
        "99.9th percentile: {:.2}",
        (metrics.histogram.value_at_quantile(0.999) as f64) / 1_000.0
    );
    info!("Max latency: {:.2}", (metrics.histogram.max() as f64) / 1_000.0);

    let new_hedging_delay = metrics.calculate_new_delay(percentile);
    info!(
        "Suggested new hedging delay ({}th percentile): {} milliseconds",
        percentile,
        new_hedging_delay
    );

    println!("All tests completed. Total time: {:?}", total_duration);
    println!("Total operations: {}", metrics.total_operations);
    println!("Total successful operations: {}", metrics.total_successes);
    println!("Total failed operations: {}", metrics.total_failures);
    println!("First requests won: {}", metrics.first_requests_won);
    println!("Second requests won: {}", metrics.second_requests_won);
    println!("Second requests sent: {}", metrics.second_requests_sent);
    println!("");
}

fn log_metrics(metrics: &Metrics, phase: &str) {
    info!("{} total operations: {}", phase, metrics.total_operations);
    info!("{} successful operations: {}", phase, metrics.total_successes);
    info!("{} failed operations: {}", phase, metrics.total_failures);
    info!("{} first requests won: {}", phase, metrics.first_requests_won);
    info!("{} second requests won: {}", phase, metrics.second_requests_won);
    info!("{} second requests sent: {}", phase, metrics.second_requests_sent);
}
