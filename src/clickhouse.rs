//! ClickHouse HTTP client: schema bootstrap and `JSONEachRow` inserts.
//!
//! The HTTP interface is used on purpose: it needs no native protocol crate,
//! the payloads are trivial to inspect in logs/tests, and it works through
//! reverse proxies. Authentication uses the `X-ClickHouse-User`/`-Key`
//! headers, so credentials never end up in URLs.

use std::collections::HashMap;

use tokio::sync::watch;
use tracing::{error, info};

use crate::backoff::Backoff;
use crate::config::ClickHouseConfig;
use crate::error::{Error, Result};

/// Columns this build expects. A table created by the old Python ingester lacks
/// `time_fired`; ClickHouse silently ignores unknown input fields, so without
/// this check the data would be written with a missing column and millisecond
/// precision without any error.
const EXPECTED_COLUMNS: [&str; 8] = [
    "entity_id",
    "state",
    "state_float",
    "attributes",
    "last_changed",
    "last_updated",
    "time_fired",
    "ingested_at",
];

/// ClickHouse HTTP client.
#[derive(Clone)]
pub struct ClickHouse {
    client: reqwest::Client,
    base: url::Url,
    user: String,
    password: String,
    database: String,
    table: String,
    retention_days: Option<u32>,
}

impl ClickHouse {
    /// Build a client from configuration.
    pub fn new(config: &ClickHouseConfig) -> Result<Self> {
        // `no_proxy()` is deliberate: ClickHouse is usually on the LAN and a
        // proxy inherited from the host would break the connection.
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(std::time::Duration::from_secs(10))
            .no_proxy()
            .user_agent(concat!("ha-ch-ingestor/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let mut base = config.url.clone();
        base.query_pairs_mut().append_pair("wait_end_of_query", "1");
        Ok(Self {
            client,
            base,
            user: config.user.clone(),
            password: config.password.clone(),
            database: config.database.clone(),
            table: config.table.clone(),
            retention_days: config.retention_days,
        })
    }

    /// Create the database and table if they do not exist.
    pub async fn ensure_schema(&self) -> Result<()> {
        let database = quote_ident(&self.database);
        self.execute(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
            .await?;
        self.execute(&self.schema_ddl()).await?;
        info!(
            database = %self.database,
            table = %self.table,
            "clickhouse schema ready"
        );
        Ok(())
    }

    /// Retry [`ensure_schema`](Self::ensure_schema) until it succeeds or
    /// shutdown is requested. A missing ClickHouse must not crash the process.
    pub async fn ensure_schema_with_retry(
        &self,
        config: &crate::backoff::BackoffConfig,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut backoff = Backoff::new(*config);
        loop {
            if *shutdown.borrow() {
                return Err(Error::Shutdown);
            }
            match self.ensure_schema().await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_transient() => {
                    let delay = backoff.next_delay();
                    error!(
                        error = %error,
                        retry_in_ms = delay.as_millis() as u64,
                        "clickhouse schema is not ready"
                    );
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = shutdown.changed() => return Err(Error::Shutdown),
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Check that an existing table has the columns this build writes.
    ///
    /// A mismatch is a hard error: ClickHouse would otherwise accept the rows
    /// and silently drop unknown fields.
    pub async fn verify_schema(&self) -> Result<()> {
        let sql = format!(
            "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' \
             FORMAT TabSeparatedRaw",
            escape_literal(&self.database),
            escape_literal(&self.table)
        );
        let body = self.query(&sql).await?;
        let mut columns: HashMap<&str, &str> = HashMap::new();
        for line in body.lines() {
            if let Some((name, kind)) = line.split_once('\t') {
                columns.insert(name.trim(), kind.trim());
            }
        }

        let missing: Vec<&str> = EXPECTED_COLUMNS
            .iter()
            .copied()
            .filter(|name| !columns.contains_key(name))
            .collect();
        if !missing.is_empty() {
            return Err(Error::Schema(format!(
                "table `{}`.`{}` is missing columns {missing:?}. Create a fresh table \
                 (for example `states_v2`) and set CLICKHOUSE_TABLE, or migrate it first; \
                 see README.md.",
                self.database, self.table
            )));
        }
        for (name, kind) in &columns {
            let compatible = match *name {
                "entity_id" | "state" => matches!(*kind, "String" | "LowCardinality(String)"),
                "attributes" => *kind == "String",
                "state_float" => *kind == "Nullable(Float64)",
                "last_changed" | "last_updated" | "time_fired" | "ingested_at" => {
                    matches!(
                        *kind,
                        "DateTime64(6, 'UTC')"
                            | "DateTime64(7, 'UTC')"
                            | "DateTime64(8, 'UTC')"
                            | "DateTime64(9, 'UTC')"
                    )
                }
                _ => true,
            };
            if !compatible {
                return Err(Error::Schema(format!(
                    "incompatible type {kind} for column {name}; timestamps require DateTime64(6..9, 'UTC'); see README.md"
                )));
            }
        }
        Ok(())
    }

    /// Retry [`verify_schema`](Self::verify_schema) on transient failures.
    pub async fn verify_schema_with_retry(
        &self,
        config: &crate::backoff::BackoffConfig,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut backoff = Backoff::new(*config);
        loop {
            if *shutdown.borrow() {
                return Err(Error::Shutdown);
            }
            match self.verify_schema().await {
                Ok(()) => {
                    info!(database = %self.database, table = %self.table, "clickhouse schema verified");
                    return Ok(());
                }
                Err(error) if error.is_transient() => {
                    let delay = backoff.next_delay();
                    error!(
                        error = %error,
                        retry_in_ms = delay.as_millis() as u64,
                        "could not verify the clickhouse schema yet"
                    );
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = shutdown.changed() => return Err(Error::Shutdown),
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Insert a batch and return the number of payload bytes sent.
    pub async fn insert(&self, rows: &[impl serde::Serialize]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut body = Vec::with_capacity(rows.len() * 256);
        for row in rows {
            serde_json::to_writer(&mut body, row)?;
            body.push(b'\n');
        }
        let bytes = body.len();

        let mut url = self.base.clone();
        url.query_pairs_mut()
            .append_pair("wait_for_async_insert", "1")
            .append_pair("input_format_skip_unknown_fields", "0")
            .append_pair(
                "query",
                &format!(
                    "INSERT INTO {}.{} FORMAT JSONEachRow",
                    quote_ident(&self.database),
                    quote_ident(&self.table)
                ),
            );

        let response = self
            .client
            .post(url)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        check_response(response).await?;
        Ok(bytes)
    }

    async fn execute(&self, sql: &str) -> Result<()> {
        let response = self
            .client
            .post(self.base.clone())
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(sql.to_owned())
            .send()
            .await?;
        check_response(response).await
    }

    async fn query(&self, sql: &str) -> Result<String> {
        let response = self
            .client
            .post(self.base.clone())
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(sql.to_owned())
            .send()
            .await?;
        read_response(response).await
    }

    fn schema_ddl(&self) -> String {
        let mut ddl = format!(
            r"CREATE TABLE IF NOT EXISTS {}.{}
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
ORDER BY (entity_id, last_updated)",
            quote_ident(&self.database),
            quote_ident(&self.table)
        );
        if let Some(days) = self.retention_days {
            ddl.push_str(&format!("\nTTL last_updated + INTERVAL {days} DAY"));
        }
        ddl
    }
}

async fn check_response(response: reqwest::Response) -> Result<()> {
    let body = read_response(response).await?;
    if !body.trim().is_empty() {
        return Err(Error::ClickHouse {
            status: reqwest::StatusCode::BAD_GATEWAY,
            body: body.chars().take(512).collect(),
        });
    }
    Ok(())
}

async fn read_response(response: reqwest::Response) -> Result<String> {
    let status = response.status();
    let exception = response
        .headers()
        .contains_key("X-ClickHouse-Exception-Code");
    // Headers can precede a late error; never acknowledge a batch before EOF.
    let body = response.text().await?;
    if !status.is_success()
        || exception
        || body.trim_start().starts_with("Code:")
        || body.contains("__exception__")
    {
        return Err(Error::ClickHouse {
            status: if status.is_success() {
                reqwest::StatusCode::INTERNAL_SERVER_ERROR
            } else {
                status
            },
            body: body.chars().take(512).collect(),
        });
    }
    Ok(body)
}

/// Quote a validated SQL identifier. Backticks inside the name are doubled so
/// the result is always syntactically valid.
#[must_use]
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Quote a string literal for the `system.columns` lookup.
fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_ident("states"), "`states`");
        assert_eq!(quote_ident("we`ird"), "`we``ird`");
    }

    #[test]
    fn escapes_string_literals() {
        assert_eq!(escape_literal("states"), "states");
        assert_eq!(escape_literal("it's"), "it''s");
    }

    #[test]
    fn schema_has_utc_microseconds_and_retention() {
        let config = ClickHouseConfig {
            url: url::Url::parse("http://localhost:8123").unwrap(),
            user: "default".to_owned(),
            password: String::new(),
            database: "homeassistant".to_owned(),
            table: "states".to_owned(),
            retention_days: Some(365),
            request_timeout: std::time::Duration::from_secs(5),
            backoff: crate::backoff::BackoffConfig::default(),
        };
        let client = ClickHouse::new(&config).unwrap();
        let ddl = client.schema_ddl();
        assert!(ddl.contains("DateTime64(6, 'UTC')"));
        assert!(ddl.contains("ENGINE = ReplacingMergeTree(ingested_at)"));
        assert!(ddl.contains("TTL last_updated + INTERVAL 365 DAY"));
        assert!(ddl.contains("`homeassistant`.`states`"));
    }
}
