//! Home Assistant WebSocket client.
//!
//! Protocol (see <https://developers.home-assistant.io/docs/api/websocket/>):
//!
//! 1. server sends `auth_required`
//! 2. client sends `{"type":"auth","access_token":...}`
//! 3. server sends `auth_ok` (or `auth_invalid`, which is fatal)
//! 4. client sends `subscribe_events` for `state_changed`
//! 5. server streams `{"type":"event", ...}` messages
//!
//! The connection is kept alive with the documented application-level
//! `{"type":"ping"}` command, which the server answers with `pong`. If no
//! message at all arrives for `idle_timeout`, the socket is considered dead
//! (half-open TCP) and the session is reconnected.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::backoff::Backoff;
use crate::config::{Filters, HaConfig, redacted_url};
use crate::error::{Error, Result};
use crate::row::StateRow;
use crate::stats::Stats;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = SplitSink<Ws, Message>;
type Stream = SplitStream<Ws>;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const SUBSCRIBE_ID: u64 = 1;
/// The default is 64 MiB. Attributes can be large, but this keeps one message
/// from pinning an unbounded amount of memory.
const MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

/// Consume `state_changed` events until shutdown, reconnecting on any
/// recoverable error.
pub async fn stream(
    config: HaConfig,
    filters: Filters,
    memory: Arc<tokio::sync::Semaphore>,
    memory_limit: usize,
    sender: mpsc::Sender<crate::queue::QueuedRow>,
    mut shutdown: watch::Receiver<bool>,
    stats: Arc<Stats>,
) -> Result<()> {
    let mut backoff = Backoff::new(config.backoff);

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let result = tokio::select! {
            biased;
            _ = async { let _ = shutdown.wait_for(|requested| *requested).await; } => return Ok(()),
            result = session(&config, &filters, &memory, memory_limit, &sender, &stats, &mut backoff) => result,
        };
        match result {
            Ok(()) => return Ok(()),
            Err(Error::Shutdown) => return Ok(()),
            Err(Error::Auth(message)) => {
                stats.auth_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Auth(message));
            }
            Err(error) => {
                stats.reconnects.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_delay();
                tracing::warn!(
                    error = %error,
                    retry_in_ms = delay.as_millis() as u64,
                    "home assistant session ended"
                );
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    _ = shutdown.changed() => return Ok(()),
                }
            }
        }
    }
}

async fn session(
    config: &HaConfig,
    filters: &Filters,
    memory: &Arc<tokio::sync::Semaphore>,
    memory_limit: usize,
    sender: &mpsc::Sender<crate::queue::QueuedRow>,
    stats: &Stats,
    backoff: &mut Backoff,
) -> Result<()> {
    let websocket_config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_SIZE))
        .max_frame_size(Some(MAX_MESSAGE_SIZE));
    let (mut sink, mut stream) = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        let (websocket, _response) =
            connect_async_with_config(config.ws_url.clone(), Some(websocket_config), false).await?;
        tracing::debug!(url = %redacted_url(&config.ws_url), "websocket connected");
        let (mut sink, mut stream) = websocket.split();
        authenticate(&mut sink, &mut stream, &config.token).await?;
        subscribe(&mut sink, &mut stream).await?;
        Ok::<_, Error>((sink, stream))
    })
    .await
    .map_err(|_| Error::Protocol("home assistant connection/handshake timed out".into()))??;
    backoff.reset();
    tracing::info!("subscribed to state_changed events");

    let mut ticker = tokio::time::interval(config.heartbeat);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // discard the immediately-ready first tick
    let mut last_received = Instant::now();
    let mut ping_id: u64 = SUBSCRIBE_ID + 1;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if last_received.elapsed() > config.idle_timeout {
                    return Err(Error::Idle(config.idle_timeout));
                }
                let ping = json!({"id": ping_id, "type": "ping"});
                ping_id += 1;
                tokio::time::timeout(HANDSHAKE_TIMEOUT, sink.send(Message::Text(ping.to_string().into())))
                    .await.map_err(|_| Error::Idle(HANDSHAKE_TIMEOUT))??;
            }
            incoming = stream.next() => {
                let Some(message) = incoming else {
                    return Err(Error::Protocol("websocket closed by home assistant".into()));
                };
                let message = message?;
                last_received = Instant::now();
                match message {
                    Message::Text(text) => {
                        handle_message(text.as_str(), filters, memory, memory_limit, sender, stats).await?;
                    }
                    Message::Binary(bytes) => {
                        let text = std::str::from_utf8(&bytes).map_err(|error| {
                            Error::Protocol(format!("non-utf8 websocket frame: {error}"))
                        })?;
                        handle_message(text, filters, memory, memory_limit, sender, stats).await?;
                    }
                    Message::Ping(payload) => {
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, sink.send(Message::Pong(payload)))
                            .await.map_err(|_| Error::Idle(HANDSHAKE_TIMEOUT))??;
                    }
                    Message::Close(_) => {
                        return Err(Error::Protocol("websocket closed by home assistant".into()));
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn authenticate(sink: &mut Sink, stream: &mut Stream, token: &str) -> Result<()> {
    let first = next_json(stream).await?;
    match first.get("type").and_then(Value::as_str) {
        Some("auth_required") => {
            let auth = json!({"type": "auth", "access_token": token});
            sink.send(Message::Text(auth.to_string().into())).await?;
            let reply = next_json(stream).await?;
            match reply.get("type").and_then(Value::as_str) {
                Some("auth_ok") => {
                    tracing::debug!("authenticated with home assistant");
                    Ok(())
                }
                Some("auth_invalid") => {
                    let message = reply
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("invalid access token")
                        .to_owned();
                    Err(Error::Auth(message))
                }
                other => Err(Error::Protocol(format!(
                    "unexpected authentication reply: {other:?}"
                ))),
            }
        }
        Some("auth_ok") => Ok(()),
        other => Err(Error::Protocol(format!(
            "unexpected first message: {other:?}"
        ))),
    }
}

async fn subscribe(sink: &mut Sink, stream: &mut Stream) -> Result<()> {
    let request = json!({
        "id": SUBSCRIBE_ID,
        "type": "subscribe_events",
        "event_type": "state_changed",
    });
    sink.send(Message::Text(request.to_string().into())).await?;

    loop {
        let reply = next_json(stream).await?;
        if reply.get("type").and_then(Value::as_str) != Some("result") {
            continue;
        }
        if reply.get("id").and_then(Value::as_u64) != Some(SUBSCRIBE_ID) {
            continue;
        }
        if reply.get("success").and_then(Value::as_bool) == Some(true) {
            return Ok(());
        }
        let error = reply
            .get("error")
            .map_or_else(|| "unknown error".to_owned(), Value::to_string);
        return Err(Error::Protocol(format!("subscribe_events failed: {error}")));
    }
}

async fn next_json(stream: &mut Stream) -> Result<Value> {
    loop {
        let message = tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.next())
            .await
            .map_err(|_| {
                Error::Protocol("timed out waiting for the home assistant handshake".into())
            })?
            .ok_or_else(|| {
                Error::Protocol("connection closed during the home assistant handshake".into())
            })??;
        match message {
            Message::Text(text) => return Ok(serde_json::from_str(text.as_str())?),
            Message::Binary(bytes) => return Ok(serde_json::from_slice(&bytes)?),
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => {
                return Err(Error::Protocol(
                    "connection closed during the home assistant handshake".into(),
                ));
            }
            _ => continue,
        }
    }
}

async fn handle_message(
    raw: &str,
    filters: &Filters,
    memory: &Arc<tokio::sync::Semaphore>,
    memory_limit: usize,
    sender: &mpsc::Sender<crate::queue::QueuedRow>,
    stats: &Stats,
) -> Result<()> {
    let message: Value = serde_json::from_str(raw)?;
    if message.get("type").and_then(Value::as_str) != Some("event") {
        return Ok(());
    }
    let event = &message["event"];
    if event.get("event_type").and_then(Value::as_str) != Some("state_changed") {
        return Ok(());
    }
    stats.events.fetch_add(1, Ordering::Relaxed);

    let new_state = &event["data"]["new_state"];
    if new_state.is_null() {
        stats.skipped.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }

    let time_fired = event.get("time_fired").and_then(Value::as_str);
    match StateRow::from_new_state(new_state, time_fired, filters) {
        Some(row) => {
            let queued = crate::queue::QueuedRow::reserve(row, memory, memory_limit).await?;
            sender.send(queued).await.map_err(|_| Error::Shutdown)?;
            stats.rows.fetch_add(1, Ordering::Relaxed);
        }
        None => {
            stats.skipped.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timeout_includes_the_websocket_http_upgrade() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = HaConfig {
            ws_url: url::Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap(),
            token: "test-token".into(),
            heartbeat: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(90),
            backoff: Default::default(),
        };
        let (sender, _receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            session(
                &config,
                &Filters::default(),
                &Arc::new(tokio::sync::Semaphore::new(1024)),
                1024,
                &sender,
                &Stats::default(),
                &mut Backoff::new(config.backoff),
            )
            .await
        });
        let (_connection, _) = listener.accept().await.unwrap();
        tokio::time::pause();
        tokio::time::advance(HANDSHAKE_TIMEOUT + Duration::from_secs(1)).await;
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("upgrade was not timed out")
            .unwrap();
        assert!(matches!(result, Err(Error::Protocol(message)) if message.contains("timed out")));
    }
}
