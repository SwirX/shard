pub struct RetryPolicy {
    pub max_attempts: u32,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32) -> Self {
        Self { max_attempts }
    }
}
