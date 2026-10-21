//! Minimal fake Home Assistant WebSocket server for local smoke tests.
//!
//! Build with:
//!
//! ```text
//! cargo run --features mock --bin mock_ha
//! ```
//!
//! Environment:
//! * `MOCK_HA_ADDR` (default `0.0.0.0:8124`)
//! * `MOCK_HA_INTERVAL_MS` (default `500`)

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async};
use tracing_subscriber::EnvFilter;

type Ws = WebSocketStream<TcpStream>;
type Sink = SplitSink<Ws, Message>;
type Stream = SplitStream<Ws>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let address: SocketAddr = std::env::var("MOCK_HA_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8124".to_owned())
        .parse()?;
    let interval = Duration::from_millis(
        std::env::var("MOCK_HA_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(500),
    );

    let listener = TcpListener::bind(address).await?;
    tracing::info!(%address, ?interval, "mock home assistant listening");

    loop {
        let (stream, peer) = listener.accept().await?;
        tracing::info!(%peer, "client connected");
        tokio::spawn(async move {
            if let Err(error) = handle(stream, interval).await {
                tracing::warn!(error = %error, "session ended");
            }
        });
    }
}

async fn handle(stream: TcpStream, interval: Duration) -> Result<(), BoxError> {
    let websocket = accept_async(stream).await?;
    let (mut sink, mut stream) = websocket.split();

    send(
        &mut sink,
        json!({"type": "auth_required", "ha_version": "mock"}),
    )
    .await?;
    let auth = next_json(&mut stream).await?;
    if auth.get("type").and_then(Value::as_str) != Some("auth") {
        return Err("client did not send an auth message".into());
    }
    send(&mut sink, json!({"type": "auth_ok", "ha_version": "mock"})).await?;

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut counter = 0_u64;
    let mut subscribed = false;

    loop {
        tokio::select! {
            incoming = stream.next() => {
                let Some(message) = incoming else {
                    return Ok(());
                };
                match message? {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(text.as_str())?;
                        match value.get("type").and_then(Value::as_str) {
                            Some("subscribe_events") => {
                                let id = value.get("id").cloned().unwrap_or(json!(1));
                                send(&mut sink, json!({
                                    "id": id,
                                    "type": "result",
                                    "success": true,
                                    "result": null
                                }))
                                .await?;
                                subscribed = true;
                                tracing::info!("subscribed; emitting events every {interval:?}");
                            }
                            Some("ping") => {
                                let id = value.get("id").cloned().unwrap_or(json!(0));
                                send(&mut sink, json!({"id": id, "type": "pong"})).await?;
                            }
                            _ => {}
                        }
                    }
                    Message::Ping(payload) => {
                        sink.send(Message::Pong(payload)).await?;
                    }
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
            _ = ticker.tick() => {
                if subscribed {
                    send(&mut sink, event(counter)).await?;
                    counter += 1;
                }
            }
        }
    }
}

async fn send(sink: &mut Sink, value: Value) -> Result<(), BoxError> {
    sink.send(Message::Text(value.to_string().into())).await?;
    Ok(())
}

async fn next_json(stream: &mut Stream) -> Result<Value, BoxError> {
    loop {
        let Some(message) = stream.next().await else {
            return Err("connection closed".into());
        };
        match message? {
            Message::Text(text) => return Ok(serde_json::from_str(text.as_str())?),
            Message::Binary(bytes) => return Ok(serde_json::from_slice(&bytes)?),
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => return Err("connection closed".into()),
            _ => continue,
        }
    }
}

fn event(counter: u64) -> Value {
    let now = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("format the current time");

    // Every fifth event removes an entity; the ingester must skip it.
    if counter % 5 == 4 {
        return json!({
            "id": 1,
            "type": "event",
            "event": {
                "event_type": "state_changed",
                "data": {
                    "entity_id": "sensor.mock_removed",
                    "old_state": {},
                    "new_state": null
                },
                "time_fired": now
            }
        });
    }

    let (entity_id, state, attributes) = match counter % 3 {
        0 => (
            "sensor.mock_temperature",
            format!("{:.1}", 20.0 + f64::from((counter % 20) as u32) / 10.0),
            json!({"friendly_name": "Mock temperature", "unit_of_measurement": "°C"}),
        ),
        1 => (
            "sensor.mock_text",
            "on".to_owned(),
            json!({"friendly_name": "Mock text"}),
        ),
        _ => (
            "sensor.mock_excluded",
            "42".to_owned(),
            json!({"friendly_name": "Mock excluded"}),
        ),
    };

    json!({
        "id": 1,
        "type": "event",
        "event": {
            "event_type": "state_changed",
            "data": {
                "entity_id": entity_id,
                "old_state": {},
                "new_state": {
                    "entity_id": entity_id,
                    "state": state,
                    "attributes": attributes,
                    "last_changed": now,
                    "last_updated": now
                }
            },
            "time_fired": now
        }
    })
}
