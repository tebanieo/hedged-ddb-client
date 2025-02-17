# DynamoDB Hedged Request Client and Load Tester

This project provides a hedged request client for Amazon DynamoDB and a load testing tool to evaluate its performance. The hedged request strategy aims to reduce tail latency by sending a second request after a short delay if the first request hasn't completed.

## Features

### HedgedDynamoClient
- Implements hedged requests for DynamoDB operations
- Supports GetItem, Query, and BatchGetItem operations
- Dynamically adjustable hedging delay
- Detailed logging of request events

### Load Tester
- Multi-threaded load testing
- Warm-up phase to establish initial metrics
- Adaptive hedging delay based on percentile latency
- Real-time progress and statistics display
- Detailed metrics logging with log rotation
- Customizable test parameters

## Installation

1. Ensure you have Rust and Cargo installed on your system.
2. Clone this repository:

git clone https://github.com/tebanieo/ddb-hedged-request.git cd dynamodb-hedged-request

3. Build the project:
cargo build --release


## Usage

### HedgedDynamoClient

To use the `HedgedDynamoClient` in your own project:

```rust
use ddb_hedged_request::HedgedDynamoClient;
use rusoto_core::Region;
use rusoto_dynamodb::GetItemInput;
use std::time::Duration;

#[tokio::main]
async fn main() {
 let client = HedgedDynamoClient::new(Region::UsEast1, Duration::from_millis(50));
 
 let input = GetItemInput {
     table_name: "YourTableName".to_string(),
     // ... other parameters
 };

 let result = client.hedged_request(input).await;
 // Handle the result
}
```

Please explore the function `send_batch` so you can build your own logic, this sample implementation uses only one record. 

## Load Tester

Run the load tester with:

```bash
cargo run --release -- [OPTIONS]
```

Options

--max-threads <MAX_THREADS>: Maximum number of threads to use (default: 4)
--batch-size <BATCH_SIZE>: Number of requests per batch (default: 50)
--delay-between-batches <DELAY>: Delay between batches in milliseconds (default: 0)
--test-duration <DURATION>: Duration of the test in seconds (default: 10)
--percentile <PERCENTILE>: Percentile to use for calculating new hedging delay (default: 70.0)
--warmup-duration <DURATION>: Duration of the warm-up phase in seconds (default: 30)

Example:

```bash
cargo run --release -- --max-threads 8 --batch-size 100 --test-duration 300 --percentile 95 --warmup-duration 60

cargo run --release -- --max-threads 4 --batch-size 50 --test-duration 10 --percentile 80 --warmup-duration 5
```

## How it works

### HedgedDynamoClient

When a request is made, the client immediately sends the first request.
It then waits for a specified hedging delay.
If the first request hasn't completed after the delay, a second request is sent.
The client returns the result of whichever request completes first.
The hedging delay can be dynamically updated based on observed latencies.

### Load Tester

- Warm-up Phase: The test starts with a warm-up phase to establish initial metrics and calculate an initial hedging delay.
- Load Test: After warm-up, the main load test begins, using the calculated initial hedging delay.
- Adaptive Hedging: The hedging delay is updated every 5 seconds based on the specified percentile of request latencies.
- Multi-threading: The test uses multiple threads to generate load, with the number of threads optimized for your system.
- Progress Display: Real-time progress and statistics are displayed during the test.
- Logging: Detailed metrics are logged to files, with log rotation to manage file sizes.

### Metrics

The tool collects and reports various metrics, including:

Total operations
Successful and failed operations
First and second request wins
Number of second requests sent
Latency percentiles (50th, 90th, 95th, 99th, 99.9th)
Suggested hedging delay based on the specified percentile

### Logging

Logs are written to the log/ directory. The logging system uses a rolling file appender with the following characteristics:

Log files are rotated when they reach 10MB in size.
A maximum of 20 archived log files are kept.
The current log file is always named output.log.
Archived logs are named output-0.log, output-1.log, etc.

## Dependencies

This project uses several Rust crates, including:

tokio for async runtime
rusoto_core and rusoto_dynamodb for AWS DynamoDB interactions
async-trait for async trait implementations
uuid for generating unique request IDs
chrono for timestamps
serde_json for JSON logging
clap for command-line argument parsing
indicatif for progress bars
log4rs for logging with rotation
hdrhistogram for latency distribution tracking

# License

MIT 

# Contributing

Contributions are welcome! Please feel free to submit a Pull Request.