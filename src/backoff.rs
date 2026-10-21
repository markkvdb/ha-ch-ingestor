//! Capped exponential backoff with jitter.

use std::time::Duration;

/// Tuning for [`Backoff`].
#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    /// First delay. Each subsequent delay doubles until [`max`](Self::max).
    pub base: Duration,
    /// Upper bound for the exponential part.
    pub max: Duration,
    /// Random extra added to every delay, to avoid thundering herds.
    pub jitter: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_secs(60),
            jitter: Duration::from_millis(250),
        }
    }
}

/// Exponential backoff state. Call [`next_delay`](Self::next_delay) after a
/// failure and [`reset`](Self::reset) after a success.
#[derive(Debug, Clone)]
pub struct Backoff {
    config: BackoffConfig,
    next: Duration,
}

impl Backoff {
    /// Create a new backoff starting at `config.base`.
    #[must_use]
    pub fn new(config: BackoffConfig) -> Self {
        Self {
            config,
            next: config.base,
        }
    }

    /// Return the next delay and advance the state.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.next;
        self.next = self.next.saturating_mul(2).min(self.config.max);
        base + jitter(self.config.jitter)
    }

    /// Return to the initial delay, e.g. after a successful session.
    pub fn reset(&mut self) {
        self.next = self.config.base;
    }
}

fn jitter(max: Duration) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }
    Duration::from_millis(fastrand::u64(0..=max.as_millis() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(jitter: Duration) -> BackoffConfig {
        BackoffConfig {
            base: Duration::from_millis(100),
            max: Duration::from_millis(350),
            jitter,
        }
    }

    #[test]
    fn doubles_until_capped() {
        let mut backoff = Backoff::new(config(Duration::ZERO));
        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));
        assert_eq!(backoff.next_delay(), Duration::from_millis(350));
        assert_eq!(backoff.next_delay(), Duration::from_millis(350));
    }

    #[test]
    fn reset_restarts_the_sequence() {
        let mut backoff = Backoff::new(config(Duration::ZERO));
        backoff.next_delay();
        backoff.next_delay();
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let mut backoff = Backoff::new(config(Duration::from_millis(50)));
        let delay = backoff.next_delay();
        assert!(delay >= Duration::from_millis(100));
        assert!(delay <= Duration::from_millis(150));
    }
}
