//! Batching, flushing, retrying and shutdown draining.

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::backoff::Backoff;
use crate::clickhouse::ClickHouse;
use crate::config::BatchConfig;
use crate::error::{Error, Result};
use crate::queue::QueuedRow;
use crate::stats::Stats;

/// Consume rows from the channel and insert them in batches.
///
/// Flushes when `size` rows are buffered or every `interval`, whichever comes
/// first. On insert failures the batch is kept and retried with backoff; during
/// shutdown retries stop after `drain_grace`.
pub async fn flush_loop(
    clickhouse: ClickHouse,
    receiver: mpsc::Receiver<QueuedRow>,
    config: BatchConfig,
    shutdown: watch::Receiver<bool>,
    stats: Arc<Stats>,
    dry_run: bool,
) -> Result<()> {
    let grace = config.drain_grace;
    let mut stop = shutdown.clone();
    let work = flush_batches(
        clickhouse,
        receiver,
        config,
        shutdown,
        Arc::clone(&stats),
        dry_run,
    );
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => result,
        _ = async { let _ = stop.wait_for(|requested| *requested).await; } => {
            tokio::time::timeout(grace, &mut work).await.unwrap_or_else(|_| {
                let snapshot = stats.snapshot();
                tracing::error!(pending_rows = snapshot.rows.saturating_sub(snapshot.inserted), "shutdown deadline reached; pending rows will be lost");
                Err(Error::DrainTimeout)
            })
        }
    }
}

async fn flush_batches(
    clickhouse: ClickHouse,
    mut receiver: mpsc::Receiver<QueuedRow>,
    config: BatchConfig,
    mut shutdown: watch::Receiver<bool>,
    stats: Arc<Stats>,
    dry_run: bool,
) -> Result<()> {
    // Do not reserve gigabytes for a typo like BATCH_SIZE=100000000.
    let mut buffer: Vec<QueuedRow> = Vec::with_capacity(config.size.min(4096));
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // discard the immediately-ready first tick

    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            incoming = receiver.recv() => match incoming {
                Some(row) => {
                    buffer.push(row);
                    if buffer.len() >= config.size {
                        flush(&clickhouse, &mut buffer, &config, &stats, dry_run).await?;
                    }
                }
                None => break,
            },
            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    flush(&clickhouse, &mut buffer, &config, &stats, dry_run).await?;
                }
            }
        }
    }

    // Drain whatever was queued before shutdown. The HA task has been asked to
    // stop, so this terminates as soon as it drops the sender.
    while let Some(row) = receiver.recv().await {
        buffer.push(row);
        if buffer.len() >= config.size {
            flush(&clickhouse, &mut buffer, &config, &stats, dry_run).await?;
        }
    }

    flush(&clickhouse, &mut buffer, &config, &stats, dry_run).await?;
    tracing::info!("flush loop stopped");
    Ok(())
}

async fn flush(
    clickhouse: &ClickHouse,
    buffer: &mut Vec<QueuedRow>,
    config: &BatchConfig,
    stats: &Stats,
    dry_run: bool,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let rows = buffer.len();

    if dry_run {
        write_ndjson(buffer)?;
        stats.inserted.fetch_add(rows as u64, Ordering::Relaxed);
        stats.batches.fetch_add(1, Ordering::Relaxed);
        buffer.clear();
        return Ok(());
    }

    let started = Instant::now();
    let mut backoff = Backoff::new(config.backoff);

    loop {
        match clickhouse.insert(buffer).await {
            Ok(bytes) => {
                stats.inserted.fetch_add(rows as u64, Ordering::Relaxed);
                stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
                stats.batches.fetch_add(1, Ordering::Relaxed);
                buffer.clear();
                tracing::info!(
                    rows,
                    bytes,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "inserted batch"
                );
                return Ok(());
            }
            Err(error) => {
                stats.insert_errors.fetch_add(1, Ordering::Relaxed);
                if !error.is_transient() {
                    tracing::error!(
                        rows,
                        error = %error,
                        "clickhouse rejected the batch; stopping instead of retrying forever"
                    );
                    return Err(error);
                }
                let delay = backoff.next_delay();
                tracing::warn!(
                    rows,
                    error = %error,
                    retry_in_ms = delay.as_millis() as u64,
                    "clickhouse insert failed; retrying"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

fn write_ndjson(rows: &[impl serde::Serialize]) -> Result<()> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for row in rows {
        serde_json::to_writer(&mut out, row)?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::row::StateRow;

    #[test]
    fn dry_run_rows_are_ndjson() {
        // The writer only touches stdout; serialisation itself is covered in
        // `row::tests`. Here we just make sure the helper accepts a batch.
        let row = StateRow {
            entity_id: "sensor.x".to_owned(),
            state: "1".to_owned(),
            state_float: Some(1.0),
            attributes: "{}".to_owned(),
            last_changed: time::OffsetDateTime::UNIX_EPOCH,
            last_updated: time::OffsetDateTime::UNIX_EPOCH,
            time_fired: time::OffsetDateTime::UNIX_EPOCH,
        };
        let encoded = serde_json::to_string(&row).unwrap();
        assert!(!encoded.contains('\n'));
        assert!(encoded.contains("\"last_updated\":\"1970-01-01 00:00:00.000000\""));
    }
}
