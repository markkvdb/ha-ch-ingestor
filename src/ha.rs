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
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::backoff::Backoff;
use crate::config::{Filters, HaConfig};
use crate::error::{Error, Result};
use crate::row::StateRow;
use crate::stats::Stats;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = SplitSink<Ws, Message>;
type Stream = SplitStream<Ws>;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const SUBSCRIBE_ID: u64 = 1;

/// Consume `state_changed` events until shutdown, reconnecting on any
/// recoverable error.
pub async fn stream(
    config: HaConfig,
    filters: Filters,
    sender: mpsc::Sender<StateRow>,
    mut shutdown: watch::Receiver<bool>,
    stats: Arc<Stats>,
) -> Result<()> {
    let mut backoff = Backoff::new(config.backoff);

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match session(
            &config,
            &filters,
            &sender,
            &mut shutdown,
            &stats,
            &mut backoff,
        )
        .await
        {
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
    sender: &mpsc::Sender<StateRow>,
    shutdown: &mut watch::Receiver<bool>,
    stats: &Stats,
    backoff: &mut Backoff,
) -> Result<()> {
    let (websocket, _response) = connect_async(config.ws_url.clone()).await?;
    tracing::debug!(url = %config.ws_url, "websocket connected");
    let (mut sink, mut stream) = websocket.split();

    authenticate(&mut sink, &mut stream, &config.token).await?;
    subscribe(&mut sink, &mut stream).await?;
    backoff.reset();
    tracing::info!("subscribed to state_changed events");

    let mut ticker = tokio::time::interval(config.heartbeat);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // discard the immediately-ready first tick
    let mut last_received = Instant::now();
    let mut ping_id: u64 = SUBSCRIBE_ID + 1;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                let _ = sink.close().await;
                return Ok(());
            }
            _ = ticker.tick() => {
                if last_received.elapsed() > config.idle_timeout {
                    return Err(Error::Idle(config.idle_timeout));
                }
                let ping = json!({"id": ping_id, "type": "ping"});
                ping_id += 1;
                sink.send(Message::Text(ping.to_string().into())).await?;
            }
            incoming = stream.next() => {
                let Some(message) = incoming else {
                    return Err(Error::Protocol("websocket closed by home assistant".into()));
                };
                let message = message?;
                last_received = Instant::now();
                match message {
                    Message::Text(text) => {
                        handle_message(text.as_str(), filters, sender, stats).await?;
                    }
                    Message::Binary(bytes) => {
                        let text = std::str::from_utf8(&bytes).map_err(|error| {
                            Error::Protocol(format!("non-utf8 websocket frame: {error}"))
                        })?;
                        handle_message(text, filters, sender, stats).await?;
                    }
                    Message::Ping(payload) => {
                        sink.send(Message::Pong(payload)).await?;
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
    sender: &mpsc::Sender<StateRow>,
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
            sender.send(row).await.map_err(|_| Error::Shutdown)?;
            stats.rows.fetch_add(1, Ordering::Relaxed);
        }
        None => {
            stats.skipped.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}
