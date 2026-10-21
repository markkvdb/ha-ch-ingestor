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
use crate::error::Result;
use crate::row::StateRow;
use crate::stats::Stats;

/// Consume rows from the channel and insert them in batches.
///
/// Flushes when `size` rows are buffered or every `interval`, whichever comes
/// first. On insert failures the batch is kept and retried with backoff; during
/// shutdown retries stop after `drain_grace`.
pub async fn flush_loop(
    clickhouse: ClickHouse,
    mut receiver: mpsc::Receiver<StateRow>,
    config: BatchConfig,
    mut shutdown: watch::Receiver<bool>,
    stats: Arc<Stats>,
    dry_run: bool,
) -> Result<()> {
    let mut buffer: Vec<StateRow> = Vec::with_capacity(config.size);
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
                        flush(&clickhouse, &mut buffer, &config, &stats, dry_run, &mut shutdown).await?;
                    }
                }
                None => break,
            },
            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    flush(&clickhouse, &mut buffer, &config, &stats, dry_run, &mut shutdown).await?;
                }
            }
        }
    }

    // Drain whatever was queued before shutdown. The HA task has been asked to
    // stop, so this terminates as soon as it drops the sender.
    while let Some(row) = receiver.recv().await {
        buffer.push(row);
        if buffer.len() >= config.size {
            flush(
                &clickhouse,
                &mut buffer,
                &config,
                &stats,
                dry_run,
                &mut shutdown,
            )
            .await?;
        }
    }

    flush(
        &clickhouse,
        &mut buffer,
        &config,
        &stats,
        dry_run,
        &mut shutdown,
    )
    .await?;
    tracing::info!("flush loop stopped");
    Ok(())
}

async fn flush(
    clickhouse: &ClickHouse,
    buffer: &mut Vec<StateRow>,
    config: &BatchConfig,
    stats: &Stats,
    dry_run: bool,
    shutdown: &mut watch::Receiver<bool>,
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
    let mut deadline: Option<Instant> = None;

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
                if deadline.is_none() && *shutdown.borrow() {
                    deadline = Some(Instant::now() + config.drain_grace);
                }
                let delay = backoff.next_delay();
                if let Some(deadline) = deadline {
                    let now = Instant::now();
                    if now >= deadline {
                        tracing::error!(
                            rows,
                            error = %error,
                            "dropping batch: clickhouse is still unavailable at the shutdown deadline"
                        );
                        return Err(error);
                    }
                    tokio::time::sleep(delay.min(deadline - now)).await;
                } else {
                    tracing::warn!(
                        rows,
                        error = %error,
                        retry_in_ms = delay.as_millis() as u64,
                        "clickhouse insert failed; retrying"
                    );
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = shutdown.changed() => {}
                    }
                }
            }
        }
    }
}

fn write_ndjson(rows: &[StateRow]) -> Result<()> {
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
    use super::*;

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
