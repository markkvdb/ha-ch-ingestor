//! Cheap atomic counters, written by the tasks and read by the reporter.

use std::sync::atomic::{AtomicU64, Ordering};

/// Counters shared between the WebSocket reader, the flusher and the reporter.
#[derive(Debug, Default)]
pub struct Stats {
    /// `state_changed` events received.
    pub events: AtomicU64,
    /// Rows handed to the flusher.
    pub rows: AtomicU64,
    /// Events dropped by the transform (excluded, removed, no timestamp).
    pub skipped: AtomicU64,
    /// Rows successfully written to ClickHouse (or printed in dry-run mode).
    pub inserted: AtomicU64,
    /// Insert requests that succeeded.
    pub batches: AtomicU64,
    /// Insert attempts that failed (retried or abandoned).
    pub insert_errors: AtomicU64,
    /// WebSocket sessions that ended and were retried.
    pub reconnects: AtomicU64,
    /// Sessions rejected by Home Assistant.
    pub auth_failures: AtomicU64,
    /// Payload bytes accepted by ClickHouse.
    pub bytes: AtomicU64,
}

/// A point-in-time copy of [`Stats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StatsSnapshot {
    /// See [`Stats::events`].
    pub events: u64,
    /// See [`Stats::rows`].
    pub rows: u64,
    /// See [`Stats::skipped`].
    pub skipped: u64,
    /// See [`Stats::inserted`].
    pub inserted: u64,
    /// See [`Stats::batches`].
    pub batches: u64,
    /// See [`Stats::insert_errors`].
    pub insert_errors: u64,
    /// See [`Stats::reconnects`].
    pub reconnects: u64,
    /// See [`Stats::auth_failures`].
    pub auth_failures: u64,
    /// See [`Stats::bytes`].
    pub bytes: u64,
}

impl Stats {
    /// Take a consistent-enough snapshot for logging.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            events: self.events.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            inserted: self.inserted.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            insert_errors: self.insert_errors.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}
