use hdrhistogram::Histogram;
use serde_json::json;


#[derive(Debug, Clone)]
pub struct Metrics {
    pub histogram: Histogram<u64>,
    pub total_operations: usize,
    pub total_successes: usize,
    pub total_failures: usize,
    pub first_requests_won: usize,
    pub second_requests_won: usize,
    pub second_requests_sent: usize,
}

#[derive(Debug, Clone)]
pub struct TestRunResult {
    pub metrics: Metrics,
    pub percentile: f64,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub first_requests_won: usize,
    pub second_requests_won: usize,
    pub second_requests_sent: usize,
    pub requests_per_second: f64,
    pub success_rate: f64,
    pub hedging_rate: f64,
    pub second_request_win_rate: f64,
    pub p1_latency: f64,
    pub p25_latency: f64,
    pub p50_latency: f64,
    pub p75_latency: f64,
    pub p90_latency: f64,
    pub p95_latency: f64,
    pub p99_latency: f64,
    pub p999_latency: f64,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            histogram: Histogram::<u64>::new(3).unwrap(),
            total_operations: 0,
            total_successes: 0,
            total_failures: 0,
            first_requests_won: 0,
            second_requests_won: 0,
            second_requests_sent: 0,
        }
    }

    pub fn calculate_percentile(&self, percentile: f64) -> u64 {
        self.histogram.value_at_quantile(percentile / 100.0)
    }

    pub fn generate_test_run_result(&self, duration: std::time::Duration, percentile: f64) -> TestRunResult {
        TestRunResult {
            metrics: self.clone(),
            percentile,
            total_requests: self.total_operations,
            successful_requests: self.total_successes,
            failed_requests: self.total_failures,
            first_requests_won: self.first_requests_won,
            second_requests_won: self.second_requests_won,
            second_requests_sent: self.second_requests_sent,
            requests_per_second: self.total_operations as f64 / duration.as_secs_f64(),
            success_rate: (self.total_successes as f64 / self.total_operations as f64) * 100.0,
            hedging_rate: (self.second_requests_sent as f64 / self.total_operations as f64) * 100.0,
            second_request_win_rate: if self.second_requests_sent > 0 { (self.second_requests_won as f64 / self.second_requests_sent as f64) * 100.0 } else { 0.0 },
            p1_latency: self.calculate_percentile(1.0) as f64 / 1000.0,
            p25_latency: self.calculate_percentile(25.0) as f64 / 1000.0,
            p50_latency: self.calculate_percentile(50.0) as f64 / 1000.0,
            p75_latency: self.calculate_percentile(75.0) as f64 / 1000.0,
            p90_latency: self.calculate_percentile(90.0) as f64 / 1000.0,
            p95_latency: self.calculate_percentile(95.0) as f64 / 1000.0,
            p99_latency: self.calculate_percentile(99.0) as f64 / 1000.0,
            p999_latency: self.calculate_percentile(99.9) as f64 / 1000.0,
        }
    }

    // pub fn print_final(&self, duration: std::time::Duration) {
    //     println!("Load test completed in {:?}", duration);
    //     println!("Total requests: {}", self.total_operations);
    //     println!("Successful requests: {}", self.total_successes);
    //     println!("Failed requests: {}", self.total_failures);
    //     println!("First requests won: {}", self.first_requests_won);
    //     println!("Second requests won: {}", self.second_requests_won);
    //     println!("Second requests sent: {}", self.second_requests_sent);
    //     println!("Requests per second: {:.2}", self.total_operations as f64 / duration.as_secs_f64());
    //     println!("Success rate: {:.2}%", (self.total_successes as f64 / self.total_operations as f64) * 100.0);
    //     println!("Hedging rate: {:.2}%", (self.second_requests_sent as f64 / self.total_operations as f64) * 100.0);
    //     println!("Second request win rate: {:.2}%", if self.second_requests_sent > 0 { (self.second_requests_won as f64 / self.second_requests_sent as f64) * 100.0 } else { 0.0 });
    //     println!("Latency percentiles:");
    //     println!("  50th percentile: {:.2} ms", self.calculate_percentile(50.0) as f64 / 1000.0);
    //     println!("  90th percentile: {:.2} ms", self.calculate_percentile(90.0) as f64 / 1000.0);
    //     println!("  95th percentile: {:.2} ms", self.calculate_percentile(95.0) as f64 / 1000.0);
    //     println!("  99th percentile: {:.2} ms", self.calculate_percentile(99.0) as f64 / 1000.0);
    //     println!("  99.9th percentile: {:.2} ms", self.calculate_percentile(99.9) as f64 / 1000.0);
    // }

    pub fn print_final(&self, duration: std::time::Duration) {
        let ops = self.total_operations as f64 / duration.as_secs_f64();
        let hedged_rate = (self.second_requests_sent as f64 / self.total_operations as f64) * 100.0;
        let success_rate = (self.total_successes as f64 / self.total_operations as f64) * 100.0;
        let second_win_rate = if self.second_requests_sent > 0 {
            (self.second_requests_won as f64 / self.second_requests_sent as f64) * 100.0
        } else {
            0.0
        };
    
        println!("Load Test Results:");
        println!(
            "Duration: {:?} | Total: {} | Success: {} | Failed: {} | 1st Won: {} | 2nd Won: {} | 2nd Sent: {} | OPS: {:.1}",
            duration,
            self.total_operations,
            self.total_successes,
            self.total_failures,
            self.first_requests_won,
            self.second_requests_won,
            self.second_requests_sent,
            ops
        );
        println!(
            "Success: {:.1}% | Hedged: {:.1}% | 2nd Won: {:.1}% | P50: {:.1}ms | P90: {:.1}ms | P95: {:.1}ms | P99: {:.1}ms | P99.9: {:.1}ms",
            success_rate,
            hedged_rate,
            second_win_rate,
            self.calculate_percentile(50.0) as f64 / 1000.0,
            self.calculate_percentile(90.0) as f64 / 1000.0,
            self.calculate_percentile(95.0) as f64 / 1000.0,
            self.calculate_percentile(99.0) as f64 / 1000.0,
            self.calculate_percentile(99.9) as f64 / 1000.0
        );
    }

    
    pub fn get_final_metrics_json(&self, duration: std::time::Duration, percentile: f64) -> serde_json::Value {
        json!({
            "duration": {
                "seconds": duration.as_secs(),
                "milliseconds": duration.as_millis()
            },
            "total_requests": self.total_operations,
            "successful_requests": self.total_successes,
            "failed_requests": self.total_failures,
            "first_requests_won": self.first_requests_won,
            "second_requests_won": self.second_requests_won,
            "second_requests_sent": self.second_requests_sent,
            "requests_per_second": self.total_operations as f64 / duration.as_secs_f64(),
            "success_rate_percentage": (self.total_successes as f64 / self.total_operations as f64) * 100.0,
            "hedging_rate_percentage": (self.second_requests_sent as f64 / self.total_operations as f64) * 100.0,
            "second_request_win_rate_percentage": (if self.second_requests_sent > 0 { (self.second_requests_won as f64 / self.second_requests_sent as f64) * 100.0 } else { 0.0 }) * 100.0,
            "latency_percentiles_ms": {
                "p1": self.calculate_percentile(1.0) as f64 / 1000.0,
                "p25": self.calculate_percentile(25.0) as f64 / 1000.0,
                "p50": self.calculate_percentile(50.0) as f64 / 1000.0,
                "p75": self.calculate_percentile(75.0) as f64 / 1000.0,
                "p90": self.calculate_percentile(90.0) as f64 / 1000.0,
                "p95": self.calculate_percentile(95.0) as f64 / 1000.0,
                "p99": self.calculate_percentile(99.0) as f64 / 1000.0,
                "p999": self.calculate_percentile(99.9) as f64 / 1000.0
            },
            "load_test_percentile": percentile
        })
    }

    
}
