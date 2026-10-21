//! Configuration loading and validation.
//!
//! Sources, in increasing priority:
//!
//! 1. built-in defaults
//! 2. `/data/options.json` (Home Assistant add-on options)
//! 3. environment variables (12-factor / Docker)
//! 4. command-line flags (see [`Overrides`])
//!
//! Secrets are never printed: [`Config::redacted_json`] is what
//! `--print-config` emits, and the manual `Debug` impls redact them too.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::time::Duration;

use serde_json::{Map, Value, json};
use url::Url;

use crate::backoff::BackoffConfig;
use crate::error::{Error, Result};

/// Where logs are written and in which shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum LogFormat {
    /// Human-readable `tracing` output.
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

impl LogFormat {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "text" | "plain" => Some(Self::Text),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

/// Command-line overrides. Everything else comes from env/options.
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    /// Print rows instead of inserting them.
    pub dry_run: bool,
    /// Override `LOG_FORMAT`.
    pub log_format: Option<LogFormat>,
}

/// Fully resolved configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Home Assistant connection settings.
    pub ha: HaConfig,
    /// ClickHouse connection settings.
    pub ch: ClickHouseConfig,
    /// Batching and buffering.
    pub batch: BatchConfig,
    /// Event filtering.
    pub filters: Filters,
    /// Run `CREATE DATABASE`/`CREATE TABLE` on startup.
    pub create_schema: bool,
    /// Print rows instead of inserting them.
    pub dry_run: bool,
    /// Log format.
    pub log_format: LogFormat,
}

/// Home Assistant WebSocket settings.
#[derive(Clone)]
pub struct HaConfig {
    /// `ws://supervisor/core/websocket` or `ws://host:8123/api/websocket`.
    pub ws_url: Url,
    /// Long-lived access token or Supervisor-injected token.
    pub token: String,
    /// How often to send a protocol-level ping.
    pub heartbeat: Duration,
    /// Reconnect when nothing was received for this long.
    pub idle_timeout: Duration,
    /// Reconnect backoff.
    pub backoff: BackoffConfig,
}

impl fmt::Debug for HaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HaConfig")
            .field("ws_url", &self.ws_url.as_str())
            .field("token", &"<redacted>")
            .field("heartbeat", &self.heartbeat)
            .field("idle_timeout", &self.idle_timeout)
            .field("backoff", &self.backoff)
            .finish()
    }
}

/// ClickHouse HTTP settings.
#[derive(Clone)]
pub struct ClickHouseConfig {
    /// Base URL of the HTTP interface, e.g. `http://clickhouse:8123`.
    pub url: Url,
    /// ClickHouse user.
    pub user: String,
    /// ClickHouse password (may be empty).
    pub password: String,
    /// Target database (created on startup unless disabled).
    pub database: String,
    /// Target table (created on startup unless disabled).
    pub table: String,
    /// Optional `TTL` for the table, in days.
    pub retention_days: Option<u32>,
    /// Whole-request timeout for inserts and DDL.
    pub request_timeout: Duration,
    /// Schema-bootstrap backoff.
    pub backoff: BackoffConfig,
}

impl fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("url", &self.url.as_str())
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("database", &self.database)
            .field("table", &self.table)
            .field("retention_days", &self.retention_days)
            .field("request_timeout", &self.request_timeout)
            .field("backoff", &self.backoff)
            .finish()
    }
}

/// Batching, buffering and shutdown behaviour.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Flush once this many rows are buffered.
    pub size: usize,
    /// Flush at least this often.
    pub interval: Duration,
    /// Bounded channel capacity. Backpressure starts when this is full.
    pub capacity: usize,
    /// How long to keep retrying a batch during shutdown.
    pub drain_grace: Duration,
    /// Insert retry backoff.
    pub backoff: BackoffConfig,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            size: 500,
            interval: Duration::from_secs(5),
            capacity: 10_000,
            drain_grace: Duration::from_secs(15),
            backoff: BackoffConfig::default(),
        }
    }
}

/// Which events/attributes end up in ClickHouse.
#[derive(Debug, Clone)]
pub struct Filters {
    /// Include the `attributes` JSON blob.
    pub include_attributes: bool,
    /// Exact `entity_id`s to ignore.
    pub exclude: HashSet<String>,
    /// Store an empty attribute string above this many bytes.
    pub max_attributes_bytes: Option<usize>,
}

impl Default for Filters {
    fn default() -> Self {
        Self {
            include_attributes: true,
            exclude: HashSet::new(),
            max_attributes_bytes: None,
        }
    }
}

impl Config {
    /// Load configuration from `/data/options.json`, the environment and the
    /// given command-line overrides.
    pub fn load(overrides: Overrides) -> Result<Self> {
        let file = read_options_file(Path::new("/data/options.json"))?;
        let env = std::env::vars_os()
            .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
            .collect();
        Self::from_sources(&Sources { env, file }, overrides)
    }

    fn from_sources(sources: &Sources, overrides: Overrides) -> Result<Self> {
        let dry_run = overrides.dry_run || sources.boolean("DRY_RUN", "dry_run")?.unwrap_or(false);

        let ha = HaConfig {
            ws_url: resolve_ws_url(sources)?,
            token: resolve_token(sources)?,
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(90),
            backoff: BackoffConfig::default(),
        };

        let database = sources
            .text("CLICKHOUSE_DATABASE", "clickhouse_database")
            .unwrap_or_else(|| "homeassistant".to_owned());
        let table = sources
            .text("CLICKHOUSE_TABLE", "clickhouse_table")
            .unwrap_or_else(|| "states".to_owned());
        validate_identifier("CLICKHOUSE_DATABASE", &database)?;
        validate_identifier("CLICKHOUSE_TABLE", &table)?;

        let ch = ClickHouseConfig {
            url: resolve_clickhouse_url(sources, dry_run)?,
            user: sources
                .text("CLICKHOUSE_USER", "clickhouse_user")
                .unwrap_or_else(|| "default".to_owned()),
            password: sources
                .raw("CLICKHOUSE_PASSWORD", "clickhouse_password")
                .unwrap_or_default(),
            database,
            table,
            retention_days: sources
                .parse::<u32>("RETENTION_DAYS", "retention_days")?
                .filter(|days| *days > 0),
            request_timeout: Duration::from_secs(30),
            backoff: BackoffConfig::default(),
        };

        let size = sources
            .parse::<usize>("BATCH_SIZE", "batch_size")?
            .unwrap_or(500)
            .max(1);
        let interval = Duration::from_secs(
            sources
                .parse::<u64>("FLUSH_INTERVAL", "flush_interval")?
                .unwrap_or(5)
                .max(1),
        );
        let capacity = sources
            .parse::<usize>("BUFFER_CAPACITY", "buffer_capacity")?
            .unwrap_or(10_000)
            .max(size);
        let batch = BatchConfig {
            size,
            interval,
            capacity,
            ..BatchConfig::default()
        };

        let filters = Filters {
            include_attributes: sources
                .boolean("INCLUDE_ATTRIBUTES", "include_attributes")?
                .unwrap_or(true),
            exclude: sources
                .list("EXCLUDE_ENTITIES", "exclude_entities")?
                .into_iter()
                .collect(),
            max_attributes_bytes: sources
                .parse::<usize>("ATTRIBUTES_MAX_BYTES", "attributes_max_bytes")?,
        };

        let log_format = match overrides.log_format {
            Some(format) => format,
            None => match sources.text("LOG_FORMAT", "log_format") {
                Some(raw) => LogFormat::parse(&raw).ok_or_else(|| {
                    Error::Config(format!("invalid LOG_FORMAT {raw:?}, expected text or json"))
                })?,
                None => LogFormat::default(),
            },
        };

        Ok(Self {
            ha,
            ch,
            batch,
            filters,
            create_schema: sources
                .boolean("CREATE_SCHEMA", "create_schema")?
                .unwrap_or(true),
            dry_run,
            log_format,
        })
    }

    /// Render the effective configuration with secrets replaced.
    #[must_use]
    pub fn redacted_json(&self) -> Value {
        json!({
            "ha": {
                "ws_url": self.ha.ws_url.as_str(),
                "token": if self.ha.token.is_empty() { "" } else { "<redacted>" },
                "heartbeat_secs": self.ha.heartbeat.as_secs(),
                "idle_timeout_secs": self.ha.idle_timeout.as_secs(),
            },
            "clickhouse": {
                "url": self.ch.url.as_str(),
                "user": &self.ch.user,
                "password": if self.ch.password.is_empty() { "" } else { "<redacted>" },
                "database": &self.ch.database,
                "table": &self.ch.table,
                "retention_days": self.ch.retention_days,
            },
            "batch": {
                "size": self.batch.size,
                "flush_interval_secs": self.batch.interval.as_secs(),
                "capacity": self.batch.capacity,
                "drain_grace_secs": self.batch.drain_grace.as_secs(),
            },
            "filters": {
                "include_attributes": self.filters.include_attributes,
                "exclude_entities": &self.filters.exclude,
                "max_attributes_bytes": self.filters.max_attributes_bytes,
            },
            "create_schema": self.create_schema,
            "dry_run": self.dry_run,
            "log_format": match self.log_format {
                LogFormat::Text => "text",
                LogFormat::Json => "json",
            },
        })
    }
}

fn resolve_clickhouse_url(sources: &Sources, dry_run: bool) -> Result<Url> {
    let url = match sources.text("CLICKHOUSE_URL", "clickhouse_url") {
        Some(raw) => Url::parse(&raw)?,
        None => {
            let Some(host) = sources.text("CLICKHOUSE_HOST", "clickhouse_host") else {
                if dry_run {
                    // Dry-run never talks to ClickHouse, so the address is moot.
                    return Ok(Url::parse("http://127.0.0.1:8123")?);
                }
                return Err(Error::Config(
                    "CLICKHOUSE_URL or CLICKHOUSE_HOST must be configured".to_owned(),
                ));
            };
            let port = sources
                .parse::<u16>("CLICKHOUSE_PORT", "clickhouse_port")?
                .unwrap_or(8123);
            let secure = sources
                .boolean("CLICKHOUSE_SECURE", "clickhouse_secure")?
                .unwrap_or(false);
            Url::parse(&format!(
                "{}://{host}:{port}",
                if secure { "https" } else { "http" }
            ))?
        }
    };
    ensure_scheme(&url, &["http", "https"], "CLICKHOUSE_URL")?;
    Ok(url)
}

fn resolve_ws_url(sources: &Sources) -> Result<Url> {
    let url = if let Some(raw) = sources.text("HA_WS_URL", "ha_ws_url") {
        Url::parse(&raw)?
    } else if let Some(raw) = sources.text("HA_URL", "ha_url") {
        url_from_http(&raw)?
    } else if sources.has_env("SUPERVISOR_TOKEN") {
        Url::parse("ws://supervisor/core/websocket")?
    } else {
        return Err(Error::Config(
            "HA_WS_URL or HA_URL must be configured (standalone), or run as a Supervisor add-on"
                .to_owned(),
        ));
    };
    ensure_scheme(&url, &["ws", "wss"], "HA_WS_URL")?;
    Ok(url)
}

fn resolve_token(sources: &Sources) -> Result<String> {
    if let Some(token) = sources.text("HA_TOKEN", "ha_token") {
        return Ok(token);
    }
    if let Some(path) = sources.text("HA_TOKEN_FILE", "ha_token_file") {
        let token = std::fs::read_to_string(&path).map_err(|error| {
            Error::Config(format!("could not read HA_TOKEN_FILE {path:?}: {error}"))
        })?;
        let token = token.trim().to_owned();
        if token.is_empty() {
            return Err(Error::Config(format!("HA_TOKEN_FILE {path:?} is empty")));
        }
        return Ok(token);
    }
    sources
        .text(
            "SUPERVISOR_TOKEN",
            "__supervisor_token_is_never_in_options_json",
        )
        .ok_or_else(|| Error::Config("HA_TOKEN (or HA_TOKEN_FILE) must be configured".to_owned()))
}

fn url_from_http(raw: &str) -> Result<Url> {
    let mut url = Url::parse(raw)?;
    let scheme = match url.scheme() {
        "http" | "ws" => "ws",
        "https" | "wss" => "wss",
        other => {
            return Err(Error::Config(format!(
                "HA_URL scheme {other:?} is not http(s)"
            )));
        }
    };
    url.set_scheme(scheme)
        .map_err(|()| Error::Config("could not derive websocket scheme from HA_URL".to_owned()))?;
    url.set_path("/api/websocket");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn ensure_scheme(url: &Url, allowed: &[&str], name: &str) -> Result<()> {
    if allowed.contains(&url.scheme()) {
        Ok(())
    } else {
        Err(Error::Config(format!(
            "{name} must use one of {allowed:?}, got {:?}",
            url.scheme()
        )))
    }
}

fn validate_identifier(kind: &str, name: &str) -> Result<()> {
    let mut chars = name.chars();
    let valid_start = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_');
    let valid_rest = chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid_start && valid_rest {
        Ok(())
    } else {
        Err(Error::Config(format!(
            "{kind} must match [A-Za-z_][A-Za-z0-9_]*, got {name:?}"
        )))
    }
}

fn read_options_file(path: &Path) -> Result<Map<String, Value>> {
    match std::fs::read_to_string(path) {
        Ok(raw) => match serde_json::from_str(&raw)? {
            Value::Object(map) => Ok(map),
            _ => Err(Error::Config(format!(
                "{} must contain a JSON object",
                path.display()
            ))),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Map::new()),
        Err(error) => Err(Error::Config(format!(
            "could not read {}: {error}",
            path.display()
        ))),
    }
}

/// Environment + options-file accessors with the precedence rules applied.
struct Sources {
    env: HashMap<String, String>,
    file: Map<String, Value>,
}

impl Sources {
    fn has_env(&self, key: &str) -> bool {
        self.env
            .get(key)
            .is_some_and(|value| !value.trim().is_empty())
    }

    fn raw(&self, env_key: &str, file_key: &str) -> Option<String> {
        self.env.get(env_key).cloned().or_else(|| {
            self.file.get(file_key).and_then(|value| match value {
                Value::Null => None,
                Value::String(text) => Some(text.clone()),
                other => Some(other.to_string()),
            })
        })
    }

    fn text(&self, env_key: &str, file_key: &str) -> Option<String> {
        self.raw(env_key, file_key)
            .filter(|value| !value.trim().is_empty())
    }

    fn parse<T>(&self, env_key: &str, file_key: &str) -> Result<Option<T>>
    where
        T: std::str::FromStr,
        T::Err: fmt::Display,
    {
        match self.raw(env_key, file_key) {
            None => Ok(None),
            Some(raw) => raw
                .trim()
                .parse::<T>()
                .map(Some)
                .map_err(|error| Error::Config(format!("invalid {env_key} {raw:?}: {error}"))),
        }
    }

    fn boolean(&self, env_key: &str, file_key: &str) -> Result<Option<bool>> {
        match self.raw(env_key, file_key) {
            None => Ok(None),
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => Ok(Some(true)),
                "0" | "false" | "no" | "off" => Ok(Some(false)),
                other => Err(Error::Config(format!(
                    "invalid {env_key} {other:?}: expected a boolean"
                ))),
            },
        }
    }

    fn list(&self, env_key: &str, file_key: &str) -> Result<Vec<String>> {
        if let Some(raw) = self.env.get(env_key) {
            return Ok(raw
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_owned)
                .collect());
        }
        match self.file.get(file_key) {
            None | Some(Value::Null) => Ok(Vec::new()),
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| {
                    item.as_str().map(str::to_owned).ok_or_else(|| {
                        Error::Config(format!("{file_key} must be an array of strings"))
                    })
                })
                .collect(),
            Some(_) => Err(Error::Config(format!(
                "{file_key} must be an array of strings"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sources(env: &[(&str, &str)], file: Value) -> Sources {
        Sources {
            env: env
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            file: match file {
                Value::Object(map) => map,
                other => panic!("file must be an object, got {other:?}"),
            },
        }
    }

    fn minimal() -> Vec<(&'static str, &'static str)> {
        vec![
            ("CLICKHOUSE_HOST", "clickhouse.local"),
            ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
            ("HA_TOKEN", "secret-token"),
        ]
    }

    fn parse(env: &[(&str, &str)], file: Value) -> Result<Config> {
        Config::from_sources(&sources(env, file), Overrides::default())
    }

    #[test]
    fn builds_clickhouse_url_from_host_and_port() {
        let config = parse(&minimal(), json!({})).unwrap();
        assert_eq!(config.ch.url.as_str(), "http://clickhouse.local:8123/");
        assert_eq!(config.ch.database, "homeassistant");
        assert_eq!(config.ch.table, "states");
        assert_eq!(config.batch.size, 500);
        assert_eq!(config.batch.capacity, 10_000);
    }

    #[test]
    fn env_wins_over_options_file() {
        let config = parse(
            &[
                ("CLICKHOUSE_URL", "http://env:8123"),
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "env-token"),
                ("BATCH_SIZE", "7"),
                ("EXCLUDE_ENTITIES", "sensor.a, sensor.b"),
            ],
            json!({
                "clickhouse_url": "http://file:8123",
                "batch_size": 11,
                "exclude_entities": ["sensor.c"]
            }),
        )
        .unwrap();
        assert_eq!(config.ch.url.as_str(), "http://env:8123/");
        assert_eq!(config.batch.size, 7);
        assert!(config.filters.exclude.contains("sensor.a"));
        assert!(config.filters.exclude.contains("sensor.b"));
        assert!(!config.filters.exclude.contains("sensor.c"));
    }

    #[test]
    fn options_file_supplies_values() {
        let config = parse(
            &[
                ("CLICKHOUSE_URL", "http://env:8123"),
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "env-token"),
            ],
            json!({
                "batch_size": 42,
                "flush_interval": 9,
                "clickhouse_database": "ha",
                "retention_days": 30,
                "exclude_entities": ["sensor.c"]
            }),
        )
        .unwrap();
        assert_eq!(config.batch.size, 42);
        assert_eq!(config.batch.interval, Duration::from_secs(9));
        assert_eq!(config.ch.database, "ha");
        assert_eq!(config.ch.retention_days, Some(30));
        assert!(config.filters.exclude.contains("sensor.c"));
    }

    #[test]
    fn supervisor_token_selects_the_supervisor_socket() {
        let config = parse(
            &[
                ("CLICKHOUSE_HOST", "clickhouse"),
                ("SUPERVISOR_TOKEN", "supervisor-token"),
            ],
            json!({}),
        )
        .unwrap();
        assert_eq!(config.ha.ws_url.as_str(), "ws://supervisor/core/websocket");
        assert_eq!(config.ha.token, "supervisor-token");
    }

    #[test]
    fn derives_websocket_url_from_ha_url() {
        let config = parse(
            &[
                ("CLICKHOUSE_HOST", "clickhouse"),
                ("HA_URL", "https://home.example/api/"),
                ("HA_TOKEN", "token"),
            ],
            json!({}),
        )
        .unwrap();
        assert_eq!(
            config.ha.ws_url.as_str(),
            "wss://home.example/api/websocket"
        );
    }

    #[test]
    fn rejects_invalid_identifiers() {
        let error = parse(
            &[
                ("CLICKHOUSE_HOST", "clickhouse"),
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "token"),
                ("CLICKHOUSE_TABLE", "states; DROP TABLE x"),
            ],
            json!({}),
        )
        .unwrap_err();
        assert!(matches!(error, Error::Config(_)));
    }

    #[test]
    fn rejects_wrong_url_schemes() {
        let error = parse(
            &[
                ("CLICKHOUSE_URL", "ftp://clickhouse"),
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "token"),
            ],
            json!({}),
        )
        .unwrap_err();
        assert!(matches!(error, Error::Config(_)));
    }

    #[test]
    fn rejects_invalid_booleans() {
        let error = parse(
            &[
                ("CLICKHOUSE_HOST", "clickhouse"),
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "token"),
                ("INCLUDE_ATTRIBUTES", "maybe"),
            ],
            json!({}),
        )
        .unwrap_err();
        assert!(matches!(error, Error::Config(_)));
    }

    #[test]
    fn missing_clickhouse_host_is_an_error() {
        let error = parse(
            &[
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "token"),
            ],
            json!({}),
        )
        .unwrap_err();
        assert!(matches!(error, Error::Config(_)));
    }

    #[test]
    fn redacts_secrets() {
        let config = parse(
            &[
                ("CLICKHOUSE_HOST", "clickhouse"),
                ("CLICKHOUSE_PASSWORD", "hunter2"),
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "super-secret"),
            ],
            json!({}),
        )
        .unwrap();
        let rendered = config.redacted_json().to_string();
        assert!(!rendered.contains("hunter2"));
        assert!(!rendered.contains("super-secret"));
        assert!(rendered.contains("<redacted>"));
        assert!(!format!("{config:?}").contains("super-secret"));
    }

    #[test]
    fn reads_token_from_file() {
        let dir = std::env::temp_dir().join(format!("ha-ch-ingestor-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        std::fs::write(&path, "file-token\n").unwrap();

        let config = parse(
            &[
                ("CLICKHOUSE_HOST", "clickhouse"),
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN_FILE", path.to_str().unwrap()),
            ],
            json!({}),
        )
        .unwrap();
        assert_eq!(config.ha.token, "file-token");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn dry_run_does_not_require_clickhouse() {
        let config = parse(
            &[
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "token"),
            ],
            json!({"dry_run": true}),
        )
        .unwrap();
        assert!(config.dry_run);
        assert_eq!(config.ch.url.as_str(), "http://127.0.0.1:8123/");
    }

    #[test]
    fn clamps_batch_capacity_to_size() {
        let config = parse(
            &[
                ("CLICKHOUSE_HOST", "clickhouse"),
                ("HA_WS_URL", "ws://ha.local:8123/api/websocket"),
                ("HA_TOKEN", "token"),
                ("BATCH_SIZE", "50"),
                ("BUFFER_CAPACITY", "10"),
            ],
            json!({}),
        )
        .unwrap();
        assert_eq!(config.batch.capacity, 50);
    }
}
