use std::time::Duration;

pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        }
    }

    pub fn with_delays(mut self, base_delay: Duration, max_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self.max_delay = max_delay;
        self
    }

    pub fn next_delay(&self, failed_attempts: u32) -> Duration {
        let base = self.base_delay.min(self.max_delay);
        let shift = failed_attempts.saturating_sub(1).min(20);
        let scaled = base.as_millis().saturating_mul(1u128 << shift);
        let capped = scaled.min(self.max_delay.as_millis());
        Duration::from_millis(capped as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_exponentially_until_capped() {
        let policy = RetryPolicy::new(10).with_delays(Duration::from_millis(100), Duration::from_secs(2));
        assert_eq!(policy.next_delay(1), Duration::from_millis(100));
        assert_eq!(policy.next_delay(2), Duration::from_millis(200));
        assert_eq!(policy.next_delay(3), Duration::from_millis(400));
        assert_eq!(policy.next_delay(5), Duration::from_millis(1600));
        assert_eq!(policy.next_delay(6), Duration::from_secs(2));
        assert_eq!(policy.next_delay(100), Duration::from_secs(2));
    }

    #[test]
    fn base_delay_is_clamped_to_max_delay() {
        let policy =
            RetryPolicy::new(10).with_delays(Duration::from_secs(5), Duration::from_secs(2));
        assert_eq!(policy.next_delay(1), Duration::from_secs(2));
    }
}