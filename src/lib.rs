//! Home Assistant `state_changed` → ClickHouse ingester.
//!
//! The crate is split into small, testable modules:
//!
//! * [`config`] — environment/options/CLI resolution and validation
//! * [`row`] — pure transform from an HA state object to a ClickHouse row
//! * [`clickhouse`] — schema bootstrap and HTTP `JSONEachRow` inserts
//! * [`ha`] — WebSocket authentication, subscription, heartbeat, reconnect
//! * [`batch`] — bounded buffering, batching, retries and shutdown drain
//! * [`backoff`] and [`stats`] — supporting utilities
//!
//! [`run`] wires the tasks together and is the entry point used by `main` and
//! by the integration tests.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

pub mod backoff;
pub mod batch;
pub mod clickhouse;
pub mod config;
pub mod error;
pub mod ha;
pub mod row;
pub mod stats;

pub use config::Config;
pub use error::{Error, Result};
pub use stats::{Stats, StatsSnapshot};

const STATS_INTERVAL: Duration = Duration::from_secs(60);

/// Run the ingester until `shutdown` completes or a fatal error occurs.
///
/// Recoverable failures never return: the WebSocket loop reconnects, and
/// ClickHouse outages keep the batch buffered. `Auth` errors and configuration
/// mistakes are returned so the process can exit with a useful status.
pub async fn run(
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<StatsSnapshot> {
    let stats = Arc::new(Stats::default());
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);

    let signal_sender = shutdown_sender.clone();
    tokio::spawn(async move {
        shutdown.await;
        let _ = signal_sender.send(true);
    });

    let clickhouse = clickhouse::ClickHouse::new(&config.ch)?;
    if config.dry_run {
        tracing::info!("dry-run: ClickHouse will not be contacted");
    } else if config.create_schema {
        clickhouse
            .ensure_schema_with_retry(&config.ch.backoff, shutdown_receiver.clone())
            .await?;
    } else {
        tracing::info!("schema creation disabled; assuming the table already exists");
    }

    let (sender, receiver) = mpsc::channel(config.batch.capacity);

    let flusher = tokio::spawn(batch::flush_loop(
        clickhouse,
        receiver,
        config.batch.clone(),
        shutdown_receiver.clone(),
        Arc::clone(&stats),
        config.dry_run,
    ));
    let websocket = tokio::spawn(ha::stream(
        config.ha,
        config.filters,
        sender,
        shutdown_receiver.clone(),
        Arc::clone(&stats),
    ));
    let reporter = tokio::spawn(stats_reporter(
        Arc::clone(&stats),
        shutdown_receiver.clone(),
    ));

    // The WebSocket task only ends on shutdown or a fatal error. Waiting for it
    // also guarantees the sender is dropped before we drain the flusher.
    let websocket_result = websocket
        .await
        .map_err(|error| Error::Protocol(format!("home assistant task failed: {error}")))?;

    // Stop the flusher even when the WebSocket task failed on its own.
    let _ = shutdown_sender.send(true);
    let flush_result = flusher
        .await
        .map_err(|error| Error::Protocol(format!("flush task failed: {error}")))?;
    let _ = reporter.await;

    let snapshot = stats.snapshot();
    tracing::info!(
        events = snapshot.events,
        rows = snapshot.rows,
        skipped = snapshot.skipped,
        inserted = snapshot.inserted,
        batches = snapshot.batches,
        insert_errors = snapshot.insert_errors,
        reconnects = snapshot.reconnects,
        bytes = snapshot.bytes,
        "ingester stopped"
    );

    flush_result?;
    match websocket_result {
        Ok(()) => Ok(snapshot),
        Err(Error::Shutdown) => Ok(snapshot),
        Err(error) => Err(error),
    }
}

async fn stats_reporter(stats: Arc<Stats>, mut shutdown: watch::Receiver<bool>) {
    let mut ticker = tokio::time::interval(STATS_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let snapshot = stats.snapshot();
                tracing::info!(
                    events = snapshot.events,
                    rows = snapshot.rows,
                    skipped = snapshot.skipped,
                    inserted = snapshot.inserted,
                    batches = snapshot.batches,
                    insert_errors = snapshot.insert_errors,
                    reconnects = snapshot.reconnects,
                    bytes = snapshot.bytes,
                    "stats"
                );
            }
            _ = shutdown.changed() => return,
        }
    }
}
