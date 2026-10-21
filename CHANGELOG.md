# Changelog

## 0.1.0

- Initial Rust implementation
- Home Assistant WebSocket auth (`auth_required` → `auth_ok`) and
  `subscribe_events` for `state_changed`
- Protocol-level ping/pong heartbeat with a 90 s liveness timeout
- Bounded queue (10 000 rows) with batch/interval flushing and retrying inserts
- Graceful SIGTERM/SIGINT drain with a configurable grace period
- Auto-created ClickHouse schema with `DateTime64(6, 'UTC')` timestamps,
  `time_fired`, ZSTD attributes and a `ReplacingMergeTree` engine
- Optional TTL retention, entity exclusions and attribute size caps
- `--dry-run` (NDJSON) and `--print-config` (secrets redacted)
- Static musl binary in a ~8 MB `scratch` image, TLS enabled by default
- Hermetic integration tests with a fake Home Assistant and wiremock ClickHouse
