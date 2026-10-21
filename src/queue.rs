//! Account for row memory until ClickHouse acknowledges the batch.

use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{Error, Result};
use crate::row::StateRow;

#[derive(Serialize)]
#[serde(transparent)]
pub struct QueuedRow {
    row: StateRow,
    #[serde(skip)]
    _memory: OwnedSemaphorePermit,
}

impl QueuedRow {
    pub async fn reserve(row: StateRow, memory: &Arc<Semaphore>, limit: usize) -> Result<Self> {
        let bytes = std::mem::size_of::<Self>()
            + row.entity_id.capacity()
            + row.state.capacity()
            + row.attributes.capacity();
        if bytes > limit {
            return Err(Error::Protocol(format!(
                "row requires {bytes} bytes, exceeding BUFFER_MAX_BYTES={limit}"
            )));
        }
        let permits = u32::try_from(bytes)
            .map_err(|_| Error::Protocol("row exceeds the semaphore limit".into()))?;
        let permit = Arc::clone(memory)
            .acquire_many_owned(permits)
            .await
            .map_err(|_| Error::Shutdown)?;
        Ok(Self {
            row,
            _memory: permit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> StateRow {
        StateRow {
            entity_id: "sensor.x".to_owned(),
            state: "1".to_owned(),
            state_float: Some(1.0),
            attributes: "{}".to_owned(),
            last_changed: time::OffsetDateTime::UNIX_EPOCH,
            last_updated: time::OffsetDateTime::UNIX_EPOCH,
            time_fired: time::OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn permits_follow_rows_through_serialization_and_drop() {
        let memory = Arc::new(Semaphore::new(1024));
        let original = row();
        let encoded = serde_json::to_value(&original).unwrap();
        let queued = QueuedRow::reserve(original, &memory, 1024).await.unwrap();
        assert!(memory.available_permits() < 1024);
        assert_eq!(serde_json::to_value(&queued).unwrap(), encoded);
        drop(queued);
        assert_eq!(memory.available_permits(), 1024);
    }

    #[tokio::test]
    async fn full_byte_budget_blocks_until_an_acknowledged_row_is_dropped() {
        let limit = std::mem::size_of::<QueuedRow>() + 32;
        let memory = Arc::new(Semaphore::new(limit));
        let queued = QueuedRow::reserve(row(), &memory, limit).await.unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                QueuedRow::reserve(row(), &memory, limit)
            )
            .await
            .is_err()
        );
        drop(queued);
        let _next = QueuedRow::reserve(row(), &memory, limit).await.unwrap();
    }

    #[tokio::test]
    async fn oversized_row_is_rejected_without_waiting() {
        let memory = Arc::new(Semaphore::new(1));
        assert!(QueuedRow::reserve(row(), &memory, 1).await.is_err());
        assert_eq!(memory.available_permits(), 1);
    }
}
