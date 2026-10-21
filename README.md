# ha-ch-ingestor

Stream Home Assistant `state_changed` events directly into ClickHouse.

A small Rust service (tokio + rustls) that authenticates against the Home
Assistant WebSocket API, batches state changes and inserts them over the
ClickHouse HTTP interface. Ships as a **~8 MB scratch container** with no shell,
no libc and no package manager.

Replaces the Python ingestor from
[`KolkhozIO/clickhouse-hassio`](https://github.com/KolkhozIO/clickhouse-hassio/tree/main/ingestor)
with the same table name and query columns, but UTC microsecond timestamps and
a `time_fired` column.

---

## How it works

```
HA Core ──state_changed──▶ ingestor ──HTTP JSONEachRow──▶ ClickHouse
                              │
                              ├─ ping/pong heartbeat
                              ├─ reconnect with backoff
                              └─ bounded queue + retrying batches
```

* **Bounded memory.** A 10 000-row channel sits between the WebSocket reader
  and the flusher, and attributes are capped at 256 KiB per row. If ClickHouse
  is slow, backpressure reaches Home Assistant instead of growing an unbounded
  `Vec`.
* **At-least-once.** A failed insert is retried (1 → 60 s backoff) with the
  batch retained. On shutdown the queue is drained with a 15 s grace period.
* **Self-healing.** Home Assistant disconnects, half-open TCP connections
  (90 s idle timeout) and ClickHouse restarts are all recovered automatically.
* **No lost startup race.** Schema creation is retried until ClickHouse is up.

---

## Quick start

```bash
docker run -d --name ha-ch-ingestor --restart unless-stopped \
  --user "$(id -u):$(id -g)" \
  -e HA_WS_URL=ws://192.168.1.10:8123/api/websocket \
  -e HA_TOKEN_FILE=/run/secrets/ha_token \
  -v "$PWD/ha_token.txt:/run/secrets/ha_token:ro" \
  -e CLICKHOUSE_URL=http://192.168.1.10:8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=secret \
  ghcr.io/markkvdb/ha-ch-ingestor:latest
```

The token is a Home Assistant **long-lived access token**: Profile → Security →
Long-lived access tokens. `HA_TOKEN` works too, but a mounted file keeps it out
of `docker inspect` and shell history.

The image runs as UID 65532 by default. On Linux, a token file with mode `0600`
is not readable by that user, so `--user "$(id -u):$(id -g)"` makes the process
run as your host user (still non-root). Alternatively `chmod 644` the mounted
file.

`HA_URL=http://192.168.1.10:8123` is accepted as an alternative to `HA_WS_URL`;
the websocket path `/api/websocket` is derived automatically.

### Keeping it minimal

| Build | Size (arm64) | TLS |
| --- | --- | --- |
| `docker build -t ha-ch-ingestor .` | ~7.8 MB | yes |
| `docker build --build-arg FEATURES=none -t ha-ch-ingestor:slim .` | ~4 MB | no |

The slim build can only use `ws://` and `http://`. Use it when both ends are on
a trusted LAN.

---

## Schema

Created automatically unless `CREATE_SCHEMA=false`:

```sql
CREATE DATABASE IF NOT EXISTS `homeassistant`;

CREATE TABLE IF NOT EXISTS `homeassistant`.`states`
(
    entity_id    LowCardinality(String),
    state        String,
    state_float  Nullable(Float64),
    attributes   String CODEC(ZSTD(3)),
    last_changed DateTime64(6, 'UTC'),
    last_updated DateTime64(6, 'UTC'),
    time_fired   DateTime64(6, 'UTC'),
    ingested_at  DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(last_updated)
ORDER BY (entity_id, last_updated);
```

Differences from the original Python schema:

* **`DateTime64(6, 'UTC')`** everywhere: Home Assistant timestamps have
  microsecond precision and are stored in UTC. All three columns carry an
  explicit `UTC` timezone, so queries never depend on the server's local zone.
* **`time_fired`** records when Home Assistant emitted the event, which makes
  ingest lag measurable.
* **`ReplacingMergeTree(ingested_at)`** collapses duplicate rows created by
  insert retries (identical `entity_id` + `last_updated`) during merges. Add
  `FINAL` to a query when you need exact deduplication immediately.
* **`attributes` is ZSTD-compressed**, since these JSON blobs are the biggest
  part of the payload.

On startup the ingester verifies the table and **refuses to write** when
`time_fired` is missing. ClickHouse would otherwise accept the rows and quietly
drop that field.

### Migrating from the Python ingester

`CREATE TABLE IF NOT EXISTS` does not alter an existing table. The old table has
no `time_fired` and stores only milliseconds, so pointing this build at it
produces a silent schema mismatch. Either create a fresh table (for example
`states_v2`) and set `CLICKHOUSE_TABLE=states_v2`, or add the column manually:

```sql
ALTER TABLE homeassistant.states
    ADD COLUMN IF NOT EXISTS time_fired DateTime64(6, 'UTC') DEFAULT last_updated;
```

The old table keeps millisecond precision and the `MergeTree` engine, so retry
duplicates are not collapsed. A fresh table gets the full schema including
`ReplacingMergeTree`.

### Timezones

Storage is UTC; convert at query time:

```sql
SELECT
    toTimeZone(last_updated, 'Europe/Paris') AS local_time,
    state_float
FROM homeassistant.states
WHERE entity_id = 'sensor.living_room_temperature'
ORDER BY last_updated DESC
LIMIT 100;
```

The ingester converts whatever offset Home Assistant sends (`+02:00`, `Z`, …)
to UTC before writing. Naive timestamps are treated as UTC.

### Retention

Set `RETENTION_DAYS=365` before the first start to add a `TTL` clause to the
table. Because the table is created with `IF NOT EXISTS`, changing this option
later does not alter an existing table.

---

## Configuration

Environment variables (highest priority), optionally `/data/options.json`
(snake_case keys, for Home Assistant add-on style deployments), then defaults.

| Variable | Default | Description |
| --- | --- | --- |
| `HA_WS_URL` | — | `ws://host:8123/api/websocket` or `ws://supervisor/core/websocket` |
| `HA_URL` | — | Alternative to `HA_WS_URL`; derives the websocket URL |
| `HA_TOKEN` | — | Long-lived access token |
| `HA_TOKEN_FILE` | — | File containing the token (whitespace trimmed) |
| `SUPERVISOR_TOKEN` | — | Used when no `HA_TOKEN`/`HA_TOKEN_FILE` is set; selects `ws://supervisor/core/websocket` |
| `CLICKHOUSE_URL` | — | `http://host:8123` (path prefixes supported) |
| `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` | — / `8123` | Alternative to `CLICKHOUSE_URL` |
| `CLICKHOUSE_SECURE` | `false` | Use `https://` with host/port |
| `CLICKHOUSE_USER` | `default` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | *(empty)* | ClickHouse password |
| `CLICKHOUSE_DATABASE` | `homeassistant` | Target database |
| `CLICKHOUSE_TABLE` | `states` | Target table |
| `RETENTION_DAYS` | *(none)* | Adds `TTL last_updated + INTERVAL n DAY` on table creation |
| `BATCH_SIZE` | `500` | Flush after this many rows |
| `FLUSH_INTERVAL` | `5` | Flush at least every n seconds |
| `BUFFER_CAPACITY` | `10000` | Bounded queue size (clamped to `BATCH_SIZE`) |
| `INCLUDE_ATTRIBUTES` | `true` | Store the attributes JSON string |
| `EXCLUDE_ENTITIES` | *(empty)* | Comma-separated `entity_id`s to skip |
| `ATTRIBUTES_MAX_BYTES` | `262144` | Store `""` above this size instead of large blobs |
| `CREATE_SCHEMA` | `true` | Run `CREATE DATABASE`/`CREATE TABLE` on startup |
| `DRY_RUN` | `false` | Print NDJSON to stdout instead of inserting |
| `LOG_FORMAT` | `text` | `text` or `json` |
| `RUST_LOG` | `info` | `tracing` filter |

CLI:

```text
--dry-run            print rows as NDJSON on stdout (logs stay on stderr);
                     no ClickHouse connection is needed
--print-config       print the resolved config with secrets redacted
--log-format <text|json>
--help / --version
```

Exit codes: `0` clean shutdown, `78` configuration or schema error, `1` fatal
runtime error (e.g. Home Assistant rejected the token, or ClickHouse rejected
the credentials). Permanent ClickHouse errors (400/401/403) stop the process
instead of retrying forever; network errors and 5xx are retried.

### Proxies

The ClickHouse client deliberately ignores `HTTP_PROXY`/`HTTPS_PROXY`; these
targets are normally on the LAN and proxying them is a common source of `502`s.
TLS verification is always on.

---

## Development

```bash
cargo test                                   # 33 unit + 8 integration tests
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The integration tests spin up a fake Home Assistant WebSocket server and a
`wiremock` ClickHouse on localhost — no Docker, no credentials, no network.

Run against a real Home Assistant without writing to ClickHouse:

```bash
HA_WS_URL=ws://homeassistant.local:8123/api/websocket \
HA_TOKEN_FILE=../ha_token.txt \
CLICKHOUSE_URL=http://localhost:8123 \
cargo run -- --dry-run
```

### Local end-to-end

```bash
docker compose up -d --build
docker compose logs -f ingestor

curl -s http://localhost:8123/ --data-binary \
  "SELECT entity_id, count() FROM homeassistant.states GROUP BY entity_id FORMAT PrettyCompact"

docker compose down       # add -v to drop the ClickHouse volume
```

The compose stack runs ClickHouse, a Rust fake Home Assistant
(`src/bin/mock_ha.rs`) and the ingestor itself.

### Image notes

* `scratch` base, non-root UID 65532, statically linked musl binary.
* Works with `--read-only --cap-drop=ALL --security-opt no-new-privileges`.
* No shell and no `HEALTHCHECK`; watch the logs or the ClickHouse row count.
* Multi-arch builds are published from native amd64/arm64 CI runners, without
  QEMU (see `.github/workflows/image.yml`).

---

## Limitations

* `state_changed` only. `subscribe_entities` (more efficient for large
  installations) is a possible future optimization.
* Exactly-once is not attempted. `ReplacingMergeTree` handles the duplicates
  that retries can create, eventually.
* Events that arrive while the WebSocket is disconnected are missed. A snapshot
  or history backfill on reconnect is future work.
* The queue is memory-only. `SIGTERM` drains it, but a hard kill (`SIGKILL`,
  OOM) loses the buffered rows. If ClickHouse is still down after the 15 s
  shutdown grace, the remaining rows are dropped and the error is logged.
* Upgrading an existing Python-created table is not automatic; see
  [Migrating from the Python ingester](#migrating-from-the-python-ingester).
* No Prometheus endpoint; counters are logged every 60 s and at shutdown.
