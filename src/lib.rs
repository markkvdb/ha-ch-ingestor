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
const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

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
    } else {
        if config.create_schema {
            clickhouse
                .ensure_schema_with_retry(&config.ch.backoff, shutdown_receiver.clone())
                .await?;
        } else {
            tracing::info!("schema creation disabled; verifying the existing table");
        }
        // A legacy table silently swallows `time_fired`; refuse to write into it.
        clickhouse
            .verify_schema_with_retry(&config.ch.backoff, shutdown_receiver.clone())
            .await?;
    }

    let (sender, receiver) = mpsc::channel(config.batch.capacity);

    let mut flusher = tokio::spawn(batch::flush_loop(
        clickhouse,
        receiver,
        config.batch.clone(),
        shutdown_receiver.clone(),
        Arc::clone(&stats),
        config.dry_run,
    ));
    let mut websocket = tokio::spawn(ha::stream(
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

    // Watch both tasks: the WebSocket task can end on shutdown/auth failure,
    // and the flusher can end on a permanent ClickHouse error. Waiting for only
    // one of them can leave the process alive with nothing happening.
    let (websocket_ended, flusher_ended) = tokio::select! {
        result = &mut websocket => (Some(result), None),
        result = &mut flusher => (None, Some(result)),
    };

    // Tell the remaining task to stop and drain.
    let _ = shutdown_sender.send(true);
    let websocket_result = match websocket_ended {
        Some(result) => join_result(result, "home assistant"),
        None => finish_task(&mut websocket, TASK_SHUTDOWN_TIMEOUT, "home assistant").await,
    };
    let flush_result = match flusher_ended {
        Some(result) => join_result(result, "flush"),
        None => finish_task(&mut flusher, TASK_SHUTDOWN_TIMEOUT, "flush").await,
    };
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

fn join_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
    task: &str,
) -> Result<()> {
    result.map_err(|error| Error::Protocol(format!("{task} task failed: {error}")))?
}

async fn finish_task(
    handle: &mut tokio::task::JoinHandle<Result<()>>,
    timeout: Duration,
    task: &str,
) -> Result<()> {
    let result = tokio::time::timeout(timeout, handle)
        .await
        .map_err(|_| Error::Protocol(format!("{task} task did not stop in time")))?;
    join_result(result, task)
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
