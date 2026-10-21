# Changelog

## 0.1.0

- Initial Rust implementation
- Home Assistant WebSocket auth (`auth_required` → `auth_ok`) and
  `subscribe_events` for `state_changed`
- Protocol-level ping/pong heartbeat with a 90 s liveness timeout
- Bounded queue (10 000 rows) with batch/interval flushing and retrying inserts
- Graceful SIGTERM/SIGINT drain with a 15 s grace period
- Auto-created ClickHouse schema with `DateTime64(6, 'UTC')` timestamps,
  `time_fired`, ZSTD attributes and a `ReplacingMergeTree` engine
- Startup schema verification: refuses to write into a legacy table that
  silently lacks `time_fired`
- Permanent ClickHouse errors (400/401/403) stop the process instead of
  buffering forever; network errors and 5xx are retried
- 256 KiB default attribute cap and an 8 MiB WebSocket message limit
- `--print-config` and logs redact URL query strings, and URLs with embedded
  credentials are rejected
- `HA_URL` keeps reverse-proxy path prefixes; space-separated HA timestamps
  are accepted
- `wss://` integration test with a generated test CA
- CI builds both Docker variants and checks dependency licenses
- Optional TTL retention, entity exclusions and attribute size caps
- `--dry-run` (NDJSON) and `--print-config` (secrets redacted)
- Static musl binary in a ~8 MB `scratch` image, TLS enabled by default
- Hermetic integration tests with a fake Home Assistant and wiremock ClickHouse
