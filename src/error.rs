//! Error types shared by all modules.
//!
//! Every fallible operation returns [`Result`]. Errors are deliberately
//! classified so that the supervisor logic can tell "retry this" apart from
//! "this configuration is broken, stop now".

use std::time::Duration;

/// Convenient result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything that can go wrong.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid or incomplete configuration. Exiting is the only sane response.
    #[error("configuration error: {0}")]
    Config(String),

    /// A URL from the configuration could not be parsed.
    #[error("invalid url: {0}")]
    Url(#[from] url::ParseError),

    /// The HTTP request to ClickHouse itself failed (connect, timeout, TLS).
    #[error("clickhouse request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// ClickHouse answered with a non-success status. The body is truncated.
    #[error("clickhouse returned HTTP {status}: {body}")]
    ClickHouse {
        /// HTTP status code returned by ClickHouse.
        status: reqwest::StatusCode,
        /// First few hundred characters of the response body.
        body: String,
    },

    /// WebSocket transport error.
    #[error("websocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),

    /// Malformed JSON, either from Home Assistant or when encoding rows.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Home Assistant rejected the access token. Never retried.
    #[error("home assistant rejected the token: {0}")]
    Auth(String),

    /// Home Assistant sent something the protocol does not allow.
    #[error("home assistant protocol error: {0}")]
    Protocol(String),

    /// The target table exists but does not match the schema this build writes.
    #[error("schema mismatch: {0}")]
    Schema(String),

    /// Nothing was received from Home Assistant within the idle timeout.
    #[error("home assistant sent nothing for {0:?}; reconnecting")]
    Idle(Duration),

    /// A shutdown was requested while an operation was in flight.
    #[error("shutdown requested")]
    Shutdown,

    /// The shared shutdown deadline expired before all rows were acknowledged.
    #[error("shutdown drain deadline exceeded; pending rows were not acknowledged")]
    DrainTimeout,

    /// Local I/O, e.g. writing dry-run output or reading a token file.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// True when retrying the same operation can succeed.
    ///
    /// Network errors, server errors and rate limits are transient. Everything
    /// else (bad credentials, missing permission, incompatible schema) will
    /// keep failing until a human changes something.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Http(_) => true,
            Self::ClickHouse { status, .. } => {
                status.is_server_error()
                    || *status == reqwest::StatusCode::REQUEST_TIMEOUT
                    || *status == reqwest::StatusCode::TOO_MANY_REQUESTS
            }
            _ => false,
        }
    }
}
