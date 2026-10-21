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

* **Bounded buffering.** The queue and in-flight batch share a 32 MiB row-memory
  budget, plus a 10 000-row channel limit. Attributes are capped at 256 KiB per
  row. HTTP serialization and the current WebSocket message use additional memory.
  When either queue limit is reached, the reader waits instead of growing memory.
* **Retrying delivery.** A failed insert is retried (1 → 60 s backoff) with the
  batch retained. Success requires the complete HTTP response, not just a 200
  status. On shutdown, one 15 s deadline covers all draining and retries.
  This is not lossless recording: crashes and disconnected periods can lose events.
* **Self-healing.** Home Assistant disconnects, stalled connection/handshakes
  (15 s timeout), half-open TCP connections (90 s idle timeout) and ClickHouse
  restarts are recovered automatically.
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
  -e CLICKHOUSE_USER=ha_ingestor \
  -e CLICKHOUSE_PASSWORD=secret \
  -e CREATE_SCHEMA=false \
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

## Deploy on Proxmox VE (lean, no Docker)

`scripts/deploy-proxmox.sh` runs from your workstation. It builds the static
`linux/amd64` (or `arm64`) binary, then creates or updates an unprivileged
Debian LXC on a Proxmox node over SSH. Inside the LXC it installs the binary, a
systemd unit and the config file. No Docker runs on the Proxmox host or in the
container, and the container opens no ports.

Requirements: SSH to a Proxmox node as root or with passwordless sudo, local
Docker (or `--binary`), and internet access on the node for the Debian template.

```bash
./scripts/deploy-proxmox.sh \
  --pve root@192.168.1.5 \
  --ha-url http://192.168.1.10:8123 \
  --clickhouse-url http://192.168.1.20:8123
```

The Home Assistant token defaults to `../ha_token.txt`; otherwise pass
`--ha-token-file`. ClickHouse credentials come from
`--clickhouse-password-file`, `CLICKHOUSE_PASSWORD`, or an interactive prompt.
For a static IP:

```bash
./scripts/deploy-proxmox.sh ... \
  --ip 192.168.1.60/24 --gateway 192.168.1.1 --nameserver 192.168.1.1
```

Re-running the script finds the container by hostname and updates it; use
`--ctid` to target a specific container. `--dry-run` prints the plan without
touching anything, and `--help` lists every option.

The script forwards `RETENTION_DAYS`, `BUFFER_CAPACITY`, `BUFFER_MAX_BYTES`,
`INCLUDE_ATTRIBUTES`, `EXCLUDE_ENTITIES`, `ATTRIBUTES_MAX_BYTES` and `LOG_FORMAT`
from your environment into the service configuration. Retention only applies
when creating a table; updates never change retention or delete existing history.

### ClickHouse access

For production, provision the schema with an administrator, then use a dedicated
password-protected user and `--no-create-schema` (or `CREATE_SCHEMA=false`). For
example, execute as an administrator, replacing the password and subnet:

```sql
CREATE USER ha_ingestor IDENTIFIED WITH sha256_password BY 'replace-with-a-strong-password'
    HOST IP '192.168.1.0/24';
GRANT INSERT ON homeassistant.states TO ha_ingestor;
GRANT SELECT ON system.columns TO ha_ingestor;

CREATE USER ha_reader IDENTIFIED WITH sha256_password BY 'replace-with-another-strong-password'
    HOST IP '192.168.1.0/24';
GRANT SELECT ON homeassistant.states TO ha_reader;
```

Use `ha_reader` for dashboards and queries (`curl --user ha_reader` prompts for
its password).
Restrict the administrator/default account to loopback, require a password,
and restrict the database network to trusted clients. Do not use an unrestricted,
passwordless `default` account. Use HTTPS/WSS or a protected network: plaintext
HTTP/WebSocket connections expose credentials to network observers.


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

On startup the ingester verifies column names and compatible types. All four
timestamps must be `DateTime64` with precision 6–9 and explicit `UTC`; numeric
states require `Nullable(Float64)`. Missing or incompatible columns stop startup.
Unknown insert fields are rejected, and async inserts must wait for acknowledgement.

### Migrating from the Python ingester

`CREATE TABLE IF NOT EXISTS` does not alter an existing table. The old table
lacks `time_fired` and uses milliseconds, so it is rejected. Create a fresh table
(for example `states_v2`) and set `CLICKHOUSE_TABLE=states_v2`. Backfill old data
separately after checking its timezone and precision; previously discarded
microseconds cannot be recovered. Adding only `time_fired` is not sufficient.

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
| `BUFFER_MAX_BYTES` | `33554432` | Shared row-memory budget for the queue and in-flight batch; must fit at least one row |
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
cargo test
bash tests/deploy-proxmox.sh                  # no SSH or Docker needed
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
  OOM) loses the buffered rows. When the shared 15 s shutdown deadline expires,
  remaining rows are dropped and an error is returned. A small byte budget also
  shortens the outage window that fits in memory; a single row larger than the
  budget is rejected and logged.
* Upgrading an existing Python-created table is not automatic; see
  [Migrating from the Python ingester](#migrating-from-the-python-ingester).
* No Prometheus endpoint; counters are logged every 60 s and at shutdown.
