//! ClickHouse HTTP client: schema bootstrap and `JSONEachRow` inserts.
//!
//! The HTTP interface is used on purpose: it needs no native protocol crate,
//! the payloads are trivial to inspect in logs/tests, and it works through
//! reverse proxies. Authentication uses the `X-ClickHouse-User`/`-Key`
//! headers, so credentials never end up in URLs.

use tokio::sync::watch;
use tracing::{error, info};

use crate::backoff::Backoff;
use crate::config::ClickHouseConfig;
use crate::error::{Error, Result};
use crate::row::StateRow;

/// ClickHouse HTTP client.
#[derive(Debug, Clone)]
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
        Ok(Self {
            client,
            base: config.url.clone(),
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
                Err(error) => {
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
            }
        }
    }

    /// Insert a batch and return the number of payload bytes sent.
    pub async fn insert(&self, rows: &[StateRow]) -> Result<usize> {
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
        url.query_pairs_mut().append_pair(
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
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    let body: String = body.chars().take(512).collect();
    Err(Error::ClickHouse { status, body })
}

/// Quote a validated SQL identifier. Backticks inside the name are doubled so
/// the result is always syntactically valid.
#[must_use]
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
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
