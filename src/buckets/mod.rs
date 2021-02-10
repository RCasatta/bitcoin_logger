pub mod blocks;
pub mod mempool;

pub fn create_buckets_limits(increment_percent: u32, upper_limit: f64) -> Vec<f64> {
    let mut buckets_limits = vec![];
    let increment_percent = 1.0f64 + (increment_percent as f64 / 100.0f64);
    let mut current_value = 1.0f64;
    loop {
        if current_value >= upper_limit {
            break;
        }
        current_value *= increment_percent;
        buckets_limits.push(current_value);
    }
    buckets_limits
}
