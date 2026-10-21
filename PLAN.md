# Plan: `ha-ch-ingestor` — Home Assistant → ClickHouse in Rust

Status: implemented in v0.1.0 (this file is kept as the design record)
Reference implementation: [`KolkhozIO/clickhouse-hassio/ingestor`](https://github.com/KolkhozIO/clickhouse-hassio/tree/main/ingestor) (`ingestor.py`, 8.5 KB)
Style reference: [`AnswerDotAI/rustyline`](https://github.com/AnswerDotAI/rustyline) (edition 2024, module-per-concern, `thiserror`, `rustfmt.toml`, workspace lints, CI = fmt + clippy + test)

---

## 1. Goal

A single static binary in a **< 10 MB scratch container** that:

1. connects to the Home Assistant WebSocket API,
2. authenticates (Supervisor token or long-lived token),
3. subscribes to `state_changed`,
4. batch-inserts rows into ClickHouse over HTTP (`JSONEachRow`),
5. survives restarts of either side, and
6. is a **drop-in schema replacement** for the existing Python add-on.

Non-goals for v1: backfill, dedup/`ReplacingMergeTree`, Prometheus endpoint, Grafana dashboards, `subscribe_entities`.

---

## 2. Decisions (defaults; all easy to change)

| # | Decision | Default | Rationale |
|---|---|---|---|
| 1 | Deployment | **standalone Docker only** (decided) | simplest; `/data/options.json` support remains available but is not packaged as an add-on |
| 2 | Wire format | HTTP `INSERT … FORMAT JSONEachRow` | same as Python, no native-protocol client, trivially debuggable |
| 3 | Schema | **improved** `homeassistant.states` (decided): UTC microseconds, `time_fired`, ZSTD attributes, `ReplacingMergeTree` | keeps the familiar table name/layout while fixing timezone precision and retry duplicates |
| 4 | Batching | `BATCH_SIZE=500`, `FLUSH_INTERVAL=5s`, whichever first | parity with Python |
| 5 | Buffer | bounded `tokio::sync::mpsc` (default 10 000 rows) | Python's unbounded list is an OOM risk; backpressure is better |
| 6 | Delivery | at-least-once, retry forever while buffer retained | parity; duplicates tolerated (documented) |
| 7 | Config precedence | CLI > env > `/data/options.json` > defaults | standalone 12-factor + add-on native |
| 8 | TLS | on by default via `rustls` (feature `tls`) | HTTPS/wss works; `--no-default-features` gives a ~3 MB plaintext image |
| 9 | Image base | `scratch`, non-root (UID 65532), TLS on (decided) | measured ~7.8 MB on arm64; no shell, no libc, no package manager |
| 10 | Errors | `thiserror` enum; `main` maps to exit codes | normal Rust service style |
| 11 | Async | tokio (multi-thread) + one reader task + one flusher task | two tasks, one channel: simplest correct concurrency |
| 12 | Tests | unit tests inline + hermetic integration test (mock WS + mock CH) | no Docker required for CI |

---

## 3. Data flow

```
HA Core ──state_changed──▶ [ha task] ──mpsc(10k)──▶ [flush task] ──HTTP──▶ ClickHouse
                              │                        │
                              └─ ping/pong, reconnect  └─ batch by size/interval, retry
```

* The **ha task** owns the WebSocket. It reconnects with exponential backoff and never blocks on ClickHouse for long: when the channel is full it awaits `send`, which backpressures the socket.
* The **flush task** owns the buffer and the ClickHouse client. It flushes on `len >= batch_size` or on the interval. On insert failure it keeps the batch and retries with backoff.
* Shutdown (`SIGTERM`/`SIGINT`) closes the channel; the flusher drains remaining rows with a bounded grace period (default 15 s), then exits 0.

---

## 4. Repository layout

```
ha-ch-ingestor/
├── Cargo.toml
├── Cargo.lock                # committed: reproducible image builds
├── rustfmt.toml              # same knobs as rustyline
├── deny.toml                 # license allow-list
├── Dockerfile                # multi-stage → scratch
├── docker-compose.yml        # dev: clickhouse + mock HA + ingestor
├── config.json               # HA add-on manifest (optional, see §12)
├── DOCS.md                   # add-on docs (optional)
├── CHANGELOG.md
├── README.md
├── src/
│   ├── main.rs               # ~40 lines: clap, tracing, signals, exit code
│   ├── lib.rs                # pub mod + run() wiring
│   ├── config.rs             # env + options.json + overrides + validation
│   ├── error.rs              # thiserror enum, Result alias
│   ├── row.rs                # pure transform StateRow::from_new_state
│   ├── backoff.rs            # capped exponential backoff + jitter
│   ├── clickhouse.rs         # ensure_schema, insert (JSONEachRow)
│   ├── ha.rs                 # ws auth, subscribe, event loop, heartbeat
│   ├── batch.rs              # flush loop, retry, drain
│   ├── stats.rs              # atomic counters (rows in/out, errors, reconnects)
│   └── bin/mock_ha.rs        # fake HA WS for e2e (feature "mock")
├── tests/
│   ├── transform.rs
│   └── ingest.rs             # mock WS + wiremock ClickHouse
└── .github/workflows/
    ├── ci.yml
    └── image.yml
```

Estimated ~1 200 lines of Rust including tests.

---

## 5. Dependencies

```toml
[package]
name = "ha-ch-ingestor"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
license = "MIT"

[dependencies]
tokio = { version = "1.54", features = ["rt-multi-thread", "macros", "signal", "sync", "time", "net"] }
tokio-tungstenite = { version = "0.30", default-features = false, features = ["connect", "handshake", "url"] }
futures-util = "0.3"
reqwest = { version = "0.13", default-features = false, features = ["rustls"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
time = { version = "0.3", features = ["formatting", "parsing", "macros"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
thiserror = "2"
clap = { version = "4", default-features = false, features = ["std", "derive", "help", "usage", "error-context"] }
url = "2"
fastrand = "2"

[features]
default = ["tls"]
tls = [
  "tokio-tungstenite/rustls-tls-native-roots",
  "reqwest/rustls",
]
mock = []   # builds the fake-HA helper; never in the production image

[[bin]]
name = "mock_ha"
required-features = ["mock"]

[dev-dependencies]
wiremock = "0.6"
```

Notes:
* `reqwest` `default-features = false` deliberately drops `system-proxy` and `charset`: a proxy inherited from the host is a classic way to break LAN traffic. We call `.no_proxy()` explicitly and expose `HTTPS_PROXY` opt-in later if needed.
* Default `rustls` provider is `aws-lc-rs`, so the **builder** stage needs `build-base cmake perl`. Runtime is fully static; no OpenSSL.
* `tls` is feature-gated only for image size; no `cfg` in application code — the URL scheme decides.

`[profile.release]`: `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = true`, `opt-level = 3`.

---

## 6. Module specs

### `config.rs`

```rust
#[derive(Debug, Clone)]
pub struct Config {
    pub ha: HaConfig,
    pub ch: ClickHouseConfig,
    pub batch: BatchConfig,
    pub filters: Filters,
    pub create_schema: bool,
    pub dry_run: bool,
}

pub struct HaConfig {
    pub ws_url: Url,              // ws://supervisor/core/websocket | ws://host:8123/api/websocket
    pub token: String,            // never logged
    pub heartbeat: Duration,      // 30 s
    pub idle_timeout: Duration,   // 90 s
}

pub struct ClickHouseConfig {
    pub url: Url,                 // http(s)://host:8123 (path allowed for reverse proxies)
    pub user: String,
    pub password: String,         // never logged
    pub database: String,         // homeassistant
    pub table: String,            // states
}

pub struct BatchConfig {
    pub size: usize,              // 500
    pub interval: Duration,       // 5 s
    pub capacity: usize,          // 10_000 rows
}

pub struct Filters {
    pub include_attributes: bool,
    pub exclude: HashSet<String>,
    pub max_attributes_bytes: Option<usize>,
}
```

Sources and precedence: `Overrides` (clap) → env → `/data/options.json` → defaults.

| Purpose | Env | options.json |
|---|---|---|
| ClickHouse base URL | `CLICKHOUSE_URL` or `CLICKHOUSE_HOST`/`CLICKHOUSE_PORT`/`CLICKHOUSE_SECURE` | `clickhouse_url`, `clickhouse_host`, `clickhouse_port`, … |
| ClickHouse auth | `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_TABLE` | same names (snake_case) |
| HA socket | `HA_WS_URL` (default `ws://supervisor/core/websocket`) | `ha_ws_url` |
| HA token | `HA_TOKEN`, `HA_TOKEN_FILE`, `SUPERVISOR_TOKEN` (fallback) | — |
| Batching | `BATCH_SIZE`, `FLUSH_INTERVAL`, `BUFFER_CAPACITY` | same |
| Filters | `INCLUDE_ATTRIBUTES`, `EXCLUDE_ENTITIES` (comma-separated), `ATTRIBUTES_MAX_BYTES` | same |
| Misc | `CREATE_SCHEMA`, `DRY_RUN`, `LOG_FORMAT`, `RUST_LOG` | `create_schema`, `log_format` |

Validation (fail fast, exit code 78): non-empty token; `ws`/`wss` scheme; `http`/`https` scheme; `size >= 1`; `interval >= 1 s`; `capacity >= size`; SQL identifiers match `^[A-Za-z_][A-Za-z0-9_]*$` (then backtick-quoted in DDL); redacted `Debug` for token/password.

CLI: `--dry-run`, `--print-config` (redacted), `--log-format text|json`. Nothing else.

### `row.rs` (pure, heavily unit-tested)

```rust
pub struct StateRow {
    pub entity_id: String,
    pub state: String,
    pub state_float: Option<f64>,
    pub attributes: String,        // JSON-encoded string, "" when disabled
    pub last_changed: String,      // "YYYY-MM-DD HH:MM:SS.mmm" UTC
    pub last_updated: String,
}

impl StateRow {
    /// `None` = skip (missing entity_id, excluded, no last_updated).
    pub fn from_new_state(v: &serde_json::Value, f: &Filters) -> Option<Self>;
}
```

Parity rules, exactly as Python:
* `new_state == null` (entity removed) → skip.
* entity in `exclude` → skip.
* `state` as string; missing/null → `""`.
* `state_float = state.parse::<f64>()` if finite, else `null`.
* `attributes` = compact JSON of `attributes` (or `{}`); `""` if `include_attributes == false`.
* `last_changed = last_changed ?? last_updated`; skip if no `last_updated`.
* `last_updated` parsed with `time::OffsetDateTime::parse(_, &Rfc3339)`, converted to UTC, formatted with milliseconds.
* `ingested_at` is **never sent** — the table's `DEFAULT now64(3)` fills it.

### `clickhouse.rs`

```rust
pub struct ClickHouse { /* reqwest::Client, Url, user, password, db, table */ }

impl ClickHouse {
    pub fn new(cfg: &ClickHouseConfig) -> Result<Self, Error>;
    pub async fn ensure_schema(&self) -> Result<(), Error>;
    pub async fn insert(&self, rows: &[StateRow]) -> Result<usize /* bytes */, Error>;
    pub async fn insert_with_retry(&self, rows: &[StateRow], &mut Backoff, &mut watch::Receiver<bool>) -> Result<(), Error>;
}
```

* `Client::builder().timeout(30s).no_proxy().user_agent(concat!("ha-ch-ingestor/", env!("CARGO_PKG_VERSION")))`.
* DDL: `POST {base}/` with the statement as the **body** (no `query` param — ClickHouse then executes the body as the query); `Content-Type: text/plain; charset=utf-8`; auth via `X-ClickHouse-User` / `X-ClickHouse-Key`.
* Insert: `POST {base}/?query=INSERT INTO \`db\`.\`table\` FORMAT JSONEachRow` (the body is **data**, which is why the statement must be a query param), body = NDJSON, headers above + `Content-Type: application/x-ndjson`.
* Non-2xx → `Error::ClickHouse { status, body }` with body truncated to 512 chars (ClickHouse errors are plain text and extremely useful).
* Body built once into a `Vec<u8>` with `serde_json::to_writer` + `b'\n'` per row — no per-row `String`.
* Optional hardening (flag, off by default): `insert_deduplication_token=<batch uuid>` + table setting `non_replicated_deduplication_window` to suppress retry duplicates.

### `ha.rs`

```rust
pub async fn stream(
    cfg: &HaConfig,
    filters: &Filters,
    tx: mpsc::Sender<StateRow>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), Error>;
```

One session:
1. `connect_async(cfg.ws_url)`.
2. Expect `auth_required` → send `{"type":"auth","access_token":…}` → expect `auth_ok`. On `auth_invalid` return `Error::Auth` (**fatal — do not retry**; Supervisor restarting a broken config in a loop helps nobody).
3. `{"id":1,"type":"subscribe_events","event_type":"state_changed"}`; require `result.success == true`.
4. Loop with `tokio::select!` over `ws.next()`, a 30 s heartbeat ticker, and shutdown.
   * `type == "event"` → `event.data.new_state` → `StateRow::from_new_state` → `tx.send(row).await`.
   * heartbeat sends `{"id":n,"type":"ping"}`; the documented HA application-level ping. Track `last_rx`; reconnect if nothing received for 90 s.
5. Any error → `Err`; caller (`run`) sleeps `Backoff` and calls `stream` again. Re-subscription is automatic because it is inside the session function.

### `batch.rs`

```rust
pub async fn flush_loop(
    ch: ClickHouse,
    rx: mpsc::Receiver<StateRow>,
    cfg: BatchConfig,
    shutdown: watch::Receiver<bool>,
    stats: Arc<Stats>,
    dry_run: bool,
) -> Result<(), Error>;
```

Pseudocode:

```text
buf = Vec::with_capacity(size)
ticker = interval(interval)
loop:
  select! {
    Some(row) = rx.recv() => { buf.push(row); if buf.len() >= size { flush(&mut buf).await } }
    _ = ticker.tick(), if !buf.is_empty() => flush(&mut buf).await
    _ = shutdown.changed() => break
  }
# drain: while let Some(row) = rx.recv().await { buf.push(row); ... }
# final flush with a 15 s timeout, then log counters and return
```

* `flush` → `ch.insert_with_retry(&buf, …)`; on success clear buffer; on failure buffer is retained and retried forever with 1→30 s backoff while checking shutdown.
* No row dropping: memory is bounded by `capacity + batch size`; backpressure reaches HA's socket.
* `dry_run`: skip ClickHouse entirely, write NDJSON to stdout, log to stderr.

### `backoff.rs`

`Backoff { base: 1 s, max: 60 s, factor: 2 }`, `next_delay()` adds `fastrand` jitter of 0–250 ms; `reset()` after a successful session/subscribe/insert.

### `main.rs` / `lib.rs`

```rust
pub async fn run(cfg: Config, shutdown: impl Future<Output = ()> + Send + 'static) -> Result<Stats, Error>;

#[tokio::main]
async fn main() -> ExitCode {
    // clap parse → Config::load → tracing init
    // shutdown: SIGINT + SIGTERM → watch channel
    // run(cfg, shutdown) ; Err(e) => eprintln + ExitCode::FAILURE (78 for config errors)
}
```

`run` wiring: create `ClickHouse`, `ensure_schema` with backoff (retry forever, log loudly), create channel, spawn flusher, spawn HA loop, await shutdown, drop sender, await flusher with timeout, return `Stats`.

---

## 7. Schema and wire format

Unchanged from the Python add-on (drop-in):

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

All timestamps are stored in UTC with microsecond precision. Home Assistant
offsets are normalised on ingest; use `toTimeZone(last_updated, 'Europe/Paris')`
for local time. `ReplacingMergeTree(ingested_at)` collapses the duplicates that
insert retries can produce; queries that need exact dedup immediately can add
`FINAL`.

`CREATE TABLE IF NOT EXISTS` means schema changes (e.g. `RETENTION_DAYS`) only
apply to new installations.

One NDJSON line:

```json
{"entity_id":"sensor.living_room_temperature","state":"21.4","state_float":21.4,"attributes":"{\"unit_of_measurement\":\"°C\"}","last_changed":"2026-10-11 16:10:02.123","last_updated":"2026-10-11 16:10:02.123"}
```

Request:

```http
POST /?query=INSERT%20INTO%20%60homeassistant%60.%60states%60%20FORMAT%20JSONEachRow
X-ClickHouse-User: default
X-ClickHouse-Key: ***
Content-Type: application/x-ndjson

{...}
{...}
```

---

## 8. Failure semantics

| Failure | Behaviour |
|---|---|
| ClickHouse down at startup | retry DDL with 1→60 s backoff, log at error; never crash |
| ClickHouse down while ingesting | retain batch, retry forever while draining; channel fills, reader backpressures |
| CH auth error (401/403) | log the body once per attempt; keep retrying (config may be fixed with restart); no data loss while process lives |
| HA token invalid | exit non-zero immediately (`auth_invalid`) |
| HA WS drop | reconnect with jittered backoff, re-auth, re-subscribe; flush in parallel |
| HA silent connection | 90 s no message → close and reconnect (heartbeat detects half-open TCP) |
| Slow ClickHouse | bounded channel backpressure; HA may disconnect us, causing a gap (documented; optional drop-oldest mode later) |
| SIGTERM/SIGINT | stop reader, drain channel, final flush with 15 s grace, exit 0 |
| Process panic | `panic = "abort"`; Supervisor/Compose restarts it |

Exactly-once is out of scope: a retry after a lost response can duplicate a batch. Dedup is a query-time or `ReplacingMergeTree` concern.

---

## 9. Observability

* `tracing` + `RUST_LOG` (default `info,ha_ch_ingestor=info`), `LOG_FORMAT=json` for structured container logs.
* Per flush: `rows`, `bytes`, `elapsed_ms`, `retry_n`.
* Per session: `authenticated`, `subscribed`, `entity_id` of last event at debug level.
* Every 60 s: `rows_in`, `rows_inserted`, `batches`, `insert_errors`, `reconnects`, `queue_depth`, `last_event_lag_ms` (wall clock − `last_updated`).
* Exit summary line with totals.
* No metrics endpoint in v1 (keeps the image minimal).

---

## 10. Tests

**Unit** (`#[cfg(test)]` in `row.rs`, `config.rs`, `backoff.rs`):
* timestamp: `2026-10-11T18:10:02.123456+02:00` → `2026-10-11 16:10:02.123`; no fractional part; invalid → skip.
* `state_float`: `"21.4"` → `Some`, `"unknown"`/`"on"` → `None`, `"NaN"`/`"inf"` → `None`.
* excluded entity skipped; `null`/missing `new_state` skipped.
* `attributes` on/off, missing attributes → `"{}"`, over `max_attributes_bytes` → `""`.
* config precedence env > options.json > defaults; invalid identifier rejected; secrets redacted in `--print-config`.

**Integration** (`tests/ingest.rs`, no Docker):
* `TcpListener` + `tokio_tungstenite::accept_async` fake HA: asserts auth message, subscription, then emits 3 events (numeric, text, excluded) and a `new_state: null`.
* `wiremock` fake ClickHouse: captures DDL and insert requests.
* Run `run(cfg, shutdown)` in a task, trigger shutdown, then assert:
  * DDL contains `CREATE DATABASE`/`CREATE TABLE`,
  * insert body = 2 lines (excluded + null removed),
  * numeric row has `state_float`, text row has `null`,
  * attributes are a JSON *string*,
  * headers carry the configured user.
* Reconnect test: fake HA closes after one event; second session must re-auth, re-subscribe, ingest the next event (backoff set to 10 ms in config).

**Manual/e2e** (`make e2e`, optional):
`docker compose up` with real ClickHouse + `--features mock` binary in `mock_ha` mode + ingestor; assert with `curl`:
```sql
SELECT count(), countIf(state_float IS NOT NULL) FROM homeassistant.states
```

CI: `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, plus `--no-default-features` build. Add `cargo deny check licenses`.

---

## 11. Docker image

Multi-stage, no QEMU when built natively per arch (CI matrix).

```dockerfile
# syntax=docker/dockerfile:1.7
FROM rust:1-alpine AS builder
ARG FEATURES=tls
RUN apk add --no-cache build-base cmake perl ca-certificates
WORKDIR /src

# dependency layer
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    mkdir -p src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release --no-default-features $( [ "$FEATURES" = tls ] && echo --features tls ) && \
    rm -rf src target/release/ha-ch-ingestor target/release/deps/ha_ch_ingestor*

COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --no-default-features $( [ "$FEATURES" = tls ] && echo --features tls ) && \
    cp target/release/ha-ch-ingestor /out-ingestor

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out-ingestor /ingestor
USER 65532:65532
ENTRYPOINT ["/ingestor"]
```

* Rust's musl target is static by default; verify with `file` / `ldd` (must say "statically linked" / "not a dynamic executable").
* Final size target: **~6–9 MB** with TLS, **~4 MB** with `--build-arg FEATURES=none`.
* No shell → `HEALTHCHECK` impossible; use log-based monitoring. `docker run --rm image --print-config` is the smoke test.
* `docker buildx build --platform linux/amd64,linux/arm64 --push -t ghcr.io/<you>/ha-ch-ingestor:<ver> .` (native runners preferred; QEMU works for local amd64 builds on Apple Silicon, slowly).
* Add `ENTRYPOINT` only; no CMD. Works with `--read-only --cap-drop=ALL --security-opt no-new-privileges`.

`docker-compose.yml` (dev):

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:25.8-alpine
    environment:
      CLICKHOUSE_SKIP_USER_SETUP: "1"   # default user is network-locked otherwise
    ports: ["8123:8123"]
  ingestor:
    build: .
    depends_on: [clickhouse, mock-ha]
    environment:
      CLICKHOUSE_URL: http://clickhouse:8123
      HA_WS_URL: ws://mock-ha:8124
      HA_TOKEN: dev-token
      RUST_LOG: debug
```

---

## 12. HA add-on packaging (deferred — standalone Docker was chosen)

Not shipped in v0.1.0. The binary already reads `/data/options.json` and
`SUPERVISOR_TOKEN`, so packaging later is mostly a `config.json` plus an image
tag. Keep the Python add-on's option names so existing configs work:

```json
{
  "name": "ClickHouse Ingestor (Rust)",
  "version": "1.0.0",
  "slug": "clickhouse_ingestor_rs",
  "image": "ghcr.io/<you>/ha-ch-ingestor",
  "arch": ["aarch64", "amd64"],
  "init": false,
  "homeassistant_api": true,
  "options": {
    "clickhouse_host": "", "clickhouse_port": 8123,
    "clickhouse_user": "default", "clickhouse_password": "",
    "clickhouse_database": "homeassistant", "clickhouse_table": "states",
    "batch_size": 500, "flush_interval": 5,
    "include_attributes": true, "exclude_entities": []
  },
  "schema": { "...": "same as Python" }
}
```

* No `rootfs/`, no s6, no `bashio`: binary reads `/data/options.json` itself, `SUPERVISOR_TOKEN` is injected because of `homeassistant_api: true`.
* Default `HA_WS_URL` is `ws://supervisor/core/websocket` when the token comes from `SUPERVISOR_TOKEN`.
* If Supervisor's `/data/options.json` isn't readable by UID 65532 on the target, use `USER root` only in the add-on build (note in DOCS).
* Build with `FEATURES=none` (ws:// + http:// only) → ~3.5 MB add-on.

---

## 13. CI

`ci.yml`: fmt (stable; use `+nightly` only if we keep unstable rustfmt knobs), clippy `-D warnings`, `cargo test` (default + `--no-default-features`), `cargo deny check licenses`.

`image.yml`: matrix `[ubuntu-latest/amd64, ubuntu-24.04-arm/arm64]` → `docker/build-push-action` with `type=gha` cache → push per-arch tags → `docker buildx imagetools create` to publish the multi-arch manifest. Tag with `Cargo.toml` version + `latest`.

---

## 14. Milestones

| # | Deliverable | Verification | Est. |
|---|---|---|---|
| 0 | `cargo init`, toolchain (`rustup`, `rustfmt`), skeleton modules, `Cargo.toml` | `cargo build` | 0.5 h |
| 1 | `config` + `row` + unit tests | `cargo test` | 2 h |
| 2 | `clickhouse` (DDL + insert) + `--dry-run` | dry-run NDJSON against mock WS | 1.5 h |
| 3 | `ha` client (auth, subscribe, heartbeat, reconnect) | integration test with fake HA | 2 h |
| 4 | `batch` flusher, backoff, graceful shutdown, stats | tests + live run | 2 h |
| 5 | Dockerfile → scratch, compose, image size check | `docker run` against real CH | 1 h |
| 6 | Integration tests + CI | green CI | 1.5 h |
| 7 | README/DOCS + add-on `config.json` (optional) | add-on install on device | 1 h |

**v1 definition of done:** events land in the unchanged `homeassistant.states` schema; killed HA session recovers; SIGTERM flushes; image < 10 MB and runs as non-root; CI green.

---

## 15. Parity checklist with `ingestor.py`

- [x] same table/columns/engine/order/partition
- [x] `new_state == null` skipped
- [x] `exclude_entities` exact match
- [x] `include_attributes` → `""` when false
- [x] `last_changed` falls back to `last_updated`
- [x] `state_float` `null` for non-numeric
- [x] `ingested_at` left to the server default
- [x] same env var names (`CLICKHOUSE_*`, `BATCH_SIZE`, `FLUSH_INTERVAL`, `INCLUDE_ATTRIBUTES`, `EXCLUDE_ENTITIES`, `HA_WS_URL`, `SUPERVISOR_TOKEN`)
- [x] at-least-once with requeue on insert failure
- [x] reconnect with exponential backoff

Deliberate improvements: bounded buffer, heartbeat/liveness, graceful shutdown, fail-fast on bad token, identifier validation, proxy bypass, TLS, structured logs, `--dry-run`, `/data/options.json` support.

---

## 16. Gotchas (write these into README)

1. **HA token:** add-on path uses `SUPERVISOR_TOKEN`; standalone needs a long-lived token (`HA_TOKEN` or `HA_TOKEN_FILE=/run/secrets/ha_token`), created in Profile → Security.
2. **Standalone WS URL** is `ws://<ha-host>:8123/api/websocket` (not `/core/websocket`).
3. **Proxy env:** containers often inherit `HTTP_PROXY`; reqwest would send LAN traffic through it. We call `.no_proxy()`; document `NO_PROXY` if we ever opt in.
4. **ClickHouse `default` user** in the official image is network-locked unless a user/password is set → `CLICKHOUSE_SKIP_USER_SETUP=1` for dev, real user for prod.
5. **CA certs in `scratch`:** copied from the builder. `--no-default-features` builds have no TLS at all; use only on trusted LAN.
6. **aws-lc-rs build deps** (`build-base cmake perl`) are builder-only; do not copy them to the final stage.
7. **`DateTime64(3)`** truncates HA's microseconds to ms (same as Python). Changing to `DateTime64(6)` would break parity.
8. **Attributes can be large** (weather forecasts). `ATTRIBUTES_MAX_BYTES` caps them; over the cap we store `""` and count it.
9. **HA may drop slow consumers;** the channel cap trades memory for gap risk. Default 10 k rows ≈ seconds of traffic.
10. **Duplicates on retry** are expected; don't build dashboards that assume uniqueness per `(entity_id, last_updated)`.
11. **`rustfmt.toml` unstable options** (`group_imports`, `imports_granularity`, `wrap_comments`) require nightly rustfmt; either use nightly for `cargo fmt` or drop those four lines for stable.
12. **No shell in the image:** debug with `--print-config`, `--dry-run`, `docker logs`, and `RUST_LOG=debug`.

---

## 17. Resolved decisions

1. **Deployment** — standalone Docker only. The add-on manifest is deferred.
2. **Schema** — improved, with explicit UTC `DateTime64(6)` columns, `time_fired`,
   ZSTD-compressed attributes and `ReplacingMergeTree` dedup.
3. **Image base** — `scratch`, TLS included (measured ~7.8 MB on arm64).
4. **TLS** — enabled by default; `--build-arg FEATURES=none` produces a ~4 MB
   plaintext-only image.
