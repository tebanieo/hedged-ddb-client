# DynamoDB Hedged Client and Load Test

This project implements a hedged client for DynamoDB and provides a load testing framework to evaluate its performance. The hedged client sends a second request after a configurable delay if the first request hasn't returned, potentially reducing tail latency.

## Features

- Hedged DynamoDB client implementation
- Configurable hedging delay
- Load testing framework with warm-up, ramp-up, and main test phases
- Detailed metrics collection and reporting
- Support for both hedged and non-hedged tests
- CSV report generation for easy analysis

## Project Structure

```bash
src 
├── dynamodb_operations.rs # DynamoDB operation implementations 
├── hedged_client.rs # Hedged client implementation 
├── lib.rs # Library root 
├── load_test.rs # Load testing logic 
├── main.rs # Main application entry point 
├── metrics.rs # Metrics collection and reporting 
└── sample_data.json # Sample data for testing - Export to S3 from your DDB table
```

## Requirements

- Rust 1.56 or later
- AWS SDK for Rust
- DynamoDB table for testing

## Configuration

Before running the load test, make sure to:

1. Set up your AWS credentials
2. Update the DynamoDB table name in `main.rs`
3. Prepare your `sample_data.json` file with test data

## Running the Load Test

To run the load test:

cargo run --release


This will execute a series of load tests with different configurations:

- Non-hedged test
- Hedged tests with percentiles: 99.0, 90.0, 75.0, 50.0, 10.0

The test suite will run for a specified number of iterations.

## Output

The load test will generate:

1. Detailed logs in the `log` directory
2. A CSV report with test results

The CSV report includes metrics such as:

- Total requests
- Successful/failed requests
- Requests per second
- Success rate
- Hedging rate
- Latency percentiles

## Customization

You can customize the load test by modifying:

- Test durations in `LoadTestConfig`
- Number of iterations in `main.rs`
- Percentiles to test in `main.rs`

## Results

Here's the data converted to a markdown table:

| Iteration | Test Type | Percentile | Total Requests | Successful Requests | Failed Requests | First Requests Won | Second Requests Won | Second Requests Sent | Requests per Second | Success Rate | Hedging Rate | Second Request Win Rate | P1 Latency (ms) | P25 Latency (ms) | P50 Latency (ms) | P75 Latency (ms) | P90 Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | P99.9 Latency (ms) |
|-----------|-----------|------------|----------------|---------------------|-----------------|--------------------|--------------------|----------------------|---------------------|--------------|--------------|-------------------------|------------------|-------------------|-------------------|-------------------|-------------------|-------------------|-------------------|---------------------|
| 0         | Non-Hedged| 0.0        | 24785          | 24785               | 0               | 24785              | 0                  | 0                    | 82.62               | 100.00       | 0.00         | 0.00                    | 1.72             | 2.06              | 8.92              | 14.99             | 20.08             | 30.08             | 78.14             | 275.71              |
| 0         | Hedged    | 99.0       | 25610          | 25610               | 0               | 25547              | 63                 | 83                   | 85.37               | 100.00       | 0.32         | 75.90                   | 1.60             | 1.81              | 2.27              | 8.12              | 11.25             | 13.21             | 16.64             | 28.00               |
| 0         | Hedged    | 90.0       | 110704         | 110704              | 0               | 109594             | 1110               | 8187                 | 369.01              | 100.00       | 7.40         | 13.56                   | 1.64             | 1.91              | 2.62              | 3.04              | 3.92              | 4.58              | 6.67              | 12.02               |
| 0         | Hedged    | 75.0       | 137874         | 137874              | 0               | 133929             | 3945               | 19940                | 459.58              | 100.00       | 14.46        | 19.78                   | 1.54             | 1.70              | 1.93              | 2.22              | 2.98              | 4.05              | 4.75              | 10.49               |
| 0         | Hedged    | 50.0       | 143654         | 143654              | 0               | 134541             | 9113               | 39082                | 478.85              | 100.00       | 27.21        | 23.32                   | 1.46             | 1.67              | 1.78              | 2.03              | 3.48              | 3.62              | 4.01              | 7.31                |
| 0         | Hedged    | 10.0       | 101175         | 101175              | 0               | 95599              | 5576               | 70223                | 337.25              | 100.00       | 69.41        | 7.94                    | 1.71             | 2.53              | 2.73              | 3.08              | 4.49              | 5.09              | 5.44              | 7.03                |


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.
This README provides an overview of your project, its structure, how to run it, and what to expect from the output. You may want to add more specific details about your implementation or any special considerations for users of your library.
