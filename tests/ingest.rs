//! Hermetic end-to-end tests: a fake Home Assistant WebSocket server and a
//! wiremock ClickHouse. No Docker and no real credentials are involved.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures_util::{SinkExt as _, StreamExt as _};
use ha_ch_ingestor::backoff::BackoffConfig;
use ha_ch_ingestor::config::{BatchConfig, ClickHouseConfig, Config, Filters, HaConfig};
use ha_ch_ingestor::error::Error;
use ha_ch_ingestor::run;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async};
use url::Url;
use wiremock::matchers::{body_string_contains, method};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const MODERN_COLUMNS: &str = "entity_id\tLowCardinality(String)\n\
state\tString\n\
state_float\tNullable(Float64)\n\
attributes\tString\n\
last_changed\tDateTime64(6, 'UTC')\n\
last_updated\tDateTime64(6, 'UTC')\n\
time_fired\tDateTime64(6, 'UTC')\n\
ingested_at\tDateTime64(6, 'UTC')\n";

const LEGACY_COLUMNS: &str = "entity_id\tLowCardinality(String)\n\
state\tString\n\
state_float\tNullable(Float64)\n\
attributes\tString\n\
last_changed\tDateTime64(3, 'UTC')\n\
last_updated\tDateTime64(3, 'UTC')\n\
ingested_at\tDateTime64(3, 'UTC')\n";

fn backoff() -> BackoffConfig {
    BackoffConfig {
        base: Duration::from_millis(10),
        max: Duration::from_millis(50),
        jitter: Duration::ZERO,
    }
}

fn test_config(ws_url: Url, clickhouse_url: Url) -> Config {
    Config {
        ha: HaConfig {
            ws_url,
            token: "test-token".to_owned(),
            heartbeat: Duration::from_millis(100),
            idle_timeout: Duration::from_secs(2),
            backoff: backoff(),
        },
        ch: ClickHouseConfig {
            url: clickhouse_url,
            user: "default".to_owned(),
            password: String::new(),
            database: "homeassistant".to_owned(),
            table: "states".to_owned(),
            retention_days: None,
            request_timeout: Duration::from_secs(2),
            backoff: backoff(),
        },
        batch: BatchConfig {
            size: 2,
            // Long on purpose: these tests exercise size-based and shutdown
            // flushing, not the timer. A short interval made batch counts racy.
            interval: Duration::from_secs(30),
            capacity: 100,
            drain_grace: Duration::from_secs(2),
            backoff: backoff(),
        },
        filters: Filters {
            include_attributes: true,
            exclude: ["sensor.excluded".to_owned()].into_iter().collect(),
            max_attributes_bytes: None,
        },
        create_schema: true,
        dry_run: false,
        log_format: Default::default(),
    }
}

async fn schema_mock(server: &MockServer, columns: &str) {
    Mock::given(method("POST"))
        .and(body_string_contains("system.columns"))
        .respond_with(ResponseTemplate::new(200).set_body_string(columns.to_owned()))
        .with_priority(1)
        .mount(server)
        .await;
}

async fn clickhouse_mock() -> MockServer {
    let server = MockServer::start().await;
    schema_mock(&server, MODERN_COLUMNS).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .with_priority(5)
        .mount(&server)
        .await;
    server
}

#[derive(Clone)]
struct FailOnce(Arc<AtomicUsize>);

impl Respond for FailOnce {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            ResponseTemplate::new(500)
        } else {
            ResponseTemplate::new(200)
        }
    }
}

fn insert_requests(requests: &[Request]) -> Vec<&Request> {
    requests
        .iter()
        .filter(|request| {
            request
                .url
                .query()
                .unwrap_or_default()
                .contains("JSONEachRow")
        })
        .collect()
}

async fn next_json(ws: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        let message = ws
            .next()
            .await
            .expect("websocket closed during handshake")
            .expect("valid websocket frame");
        match message {
            Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
            Message::Binary(bytes) => return serde_json::from_slice(&bytes).unwrap(),
            _ => continue,
        }
    }
}

async fn handshake(ws: &mut WebSocketStream<TcpStream>) {
    ws.send(Message::Text(
        r#"{"type":"auth_required","ha_version":"test"}"#.into(),
    ))
    .await
    .unwrap();
    let auth = next_json(ws).await;
    assert_eq!(auth["type"], "auth");
    assert_eq!(auth["access_token"], "test-token");

    ws.send(Message::Text(
        r#"{"type":"auth_ok","ha_version":"test"}"#.into(),
    ))
    .await
    .unwrap();
    let subscribe = next_json(ws).await;
    assert_eq!(subscribe["type"], "subscribe_events");
    assert_eq!(subscribe["event_type"], "state_changed");

    ws.send(Message::Text(
        json!({"id": subscribe["id"], "type": "result", "success": true, "result": null})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
}

fn event(entity_id: &str, state: Value, timestamp: &str) -> Value {
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
                    "attributes": {"unit_of_measurement": "°C", "friendly_name": "Test"},
                    "last_changed": timestamp,
                    "last_updated": timestamp
                }
            },
            "time_fired": timestamp
        }
    })
}

fn removed(entity_id: &str) -> Value {
    json!({
        "id": 1,
        "type": "event",
        "event": {
            "event_type": "state_changed",
            "data": {
                "entity_id": entity_id,
                "old_state": {},
                "new_state": null
            },
            "time_fired": "2026-10-11T16:10:05+00:00"
        }
    })
}

async fn spawn_fake_ha(
    events: Vec<Value>,
    close_after_events: bool,
) -> (Url, oneshot::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (done_sender, done_receiver) = oneshot::channel();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        handshake(&mut ws).await;

        for event in events {
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        let _ = done_sender.send(());

        if close_after_events {
            let _ = ws.close(None).await;
            return;
        }
        // Keep the connection alive until the ingester closes it.
        while let Some(message) = ws.next().await {
            match message {
                Ok(Message::Ping(payload)) => {
                    let _ = ws.send(Message::Pong(payload)).await;
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    (
        Url::parse(&format!("ws://{address}/api/websocket")).unwrap(),
        done_receiver,
    )
}

#[tokio::test]
async fn ingests_and_skips_expected_events() {
    let clickhouse = clickhouse_mock().await;
    let events = vec![
        event(
            "sensor.temperature",
            json!("21.5"),
            "2026-10-11T18:10:02.123456+02:00",
        ),
        event("sensor.text", json!("on"), "2026-10-11T16:10:03.000001Z"),
        event("sensor.excluded", json!("99"), "2026-10-11T16:10:04+00:00"),
        removed("sensor.removed"),
    ];
    let (ws_url, done) = spawn_fake_ha(events, false).await;
    let config = test_config(ws_url, Url::parse(&clickhouse.uri()).unwrap());

    let notify = Arc::new(Notify::new());
    let task_notify = Arc::clone(&notify);
    let handle = tokio::spawn(run(config, async move {
        task_notify.notified().await;
    }));

    tokio::time::timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap();

    // Wait for evidence that the rows reached ClickHouse before shutting down;
    // this avoids racing the shutdown against in-flight WebSocket frames.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let requests = clickhouse.received_requests().await.unwrap();
        if requests.iter().any(|request| {
            request
                .url
                .query()
                .unwrap_or_default()
                .contains("JSONEachRow")
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the first insert"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    notify.notify_one();
    let stats = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(stats.events, 4);
    assert_eq!(stats.inserted, 2);
    assert_eq!(stats.skipped, 2);
    assert_eq!(stats.batches, 1);

    let requests = clickhouse.received_requests().await.unwrap();
    let schema: String = requests
        .iter()
        .map(|request| String::from_utf8_lossy(&request.body).into_owned())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(schema.contains("CREATE DATABASE IF NOT EXISTS `homeassistant`"));
    assert!(schema.contains("DateTime64(6, 'UTC')"));
    assert!(schema.contains("ReplacingMergeTree(ingested_at)"));

    let insert = requests
        .iter()
        .find(|request| {
            request
                .url
                .query()
                .unwrap_or_default()
                .contains("JSONEachRow")
        })
        .expect("insert request");
    let rows: Vec<Value> = String::from_utf8_lossy(&insert.body)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["entity_id"], "sensor.temperature");
    assert_eq!(rows[0]["state_float"], 21.5);
    // Offsets are normalised to UTC and keep microsecond precision.
    assert_eq!(rows[0]["last_updated"], "2026-10-11 16:10:02.123456");
    assert_eq!(rows[0]["time_fired"], "2026-10-11 16:10:02.123456");
    assert_eq!(
        rows[0]["attributes"],
        r#"{"friendly_name":"Test","unit_of_measurement":"°C"}"#
    );
    assert_eq!(rows[1]["entity_id"], "sensor.text");
    assert_eq!(rows[1]["state_float"], Value::Null);
}

#[tokio::test]
async fn reconnects_and_resubscribes() {
    let clickhouse = clickhouse_mock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (done_sender, done_receiver) = oneshot::channel();

    tokio::spawn(async move {
        let events = [
            event("sensor.one", json!("1"), "2026-10-11T16:10:01Z"),
            event("sensor.two", json!("2"), "2026-10-11T16:10:02Z"),
        ];
        for event in events {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            handshake(&mut ws).await;
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = ws.close(None).await;
        }
        let _ = done_sender.send(());
    });

    let config = test_config(
        Url::parse(&format!("ws://{address}/api/websocket")).unwrap(),
        Url::parse(&clickhouse.uri()).unwrap(),
    );
    let notify = Arc::new(Notify::new());
    let task_notify = Arc::clone(&notify);
    let handle = tokio::spawn(run(config, async move {
        task_notify.notified().await;
    }));

    tokio::time::timeout(Duration::from_secs(5), done_receiver)
        .await
        .unwrap()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let requests = clickhouse.received_requests().await.unwrap();
        if requests
            .iter()
            .any(|request| String::from_utf8_lossy(&request.body).contains("sensor.two"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the second session to be ingested"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    notify.notify_one();
    let stats = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(stats.inserted, 2);
    assert!(stats.reconnects >= 1, "expected at least one reconnect");
}

#[tokio::test]
async fn invalid_token_is_fatal() {
    let clickhouse = clickhouse_mock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        ws.send(Message::Text(
            r#"{"type":"auth_required","ha_version":"test"}"#.into(),
        ))
        .await
        .unwrap();
        let auth = next_json(&mut ws).await;
        assert_eq!(auth["type"], "auth");
        ws.send(Message::Text(
            json!({"type": "auth_invalid", "message": "bad token"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    });

    let config = test_config(
        Url::parse(&format!("ws://{address}/api/websocket")).unwrap(),
        Url::parse(&clickhouse.uri()).unwrap(),
    );
    let handle = tokio::spawn(run(config, std::future::pending::<()>()));

    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(Error::Auth(_))), "got {result:?}");
}

fn spawn_ingester(
    config: Config,
) -> (
    Arc<Notify>,
    tokio::task::JoinHandle<Result<ha_ch_ingestor::StatsSnapshot, Error>>,
) {
    let notify = Arc::new(Notify::new());
    let task_notify = Arc::clone(&notify);
    let handle = tokio::spawn(run(config, async move {
        task_notify.notified().await;
    }));
    (notify, handle)
}

#[tokio::test]
async fn retries_a_transient_insert_error() {
    let server = MockServer::start().await;
    schema_mock(&server, MODERN_COLUMNS).await;
    Mock::given(method("POST"))
        .and(body_string_contains("sensor.retry"))
        .respond_with(FailOnce(Arc::new(AtomicUsize::new(0))))
        .with_priority(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .with_priority(5)
        .mount(&server)
        .await;

    let (ws_url, _done) = spawn_fake_ha(
        vec![event("sensor.retry", json!("1"), "2026-10-11T16:10:01Z")],
        false,
    )
    .await;
    let mut config = test_config(ws_url, Url::parse(&server.uri()).unwrap());
    config.batch.size = 1;
    let (notify, handle) = spawn_ingester(config);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let requests = server.received_requests().await.unwrap();
        if insert_requests(&requests).len() >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the insert was not retried after a 500"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    notify.notify_one();
    let stats = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(stats.inserted, 1);
    assert_eq!(stats.insert_errors, 1);

    let requests = server.received_requests().await.unwrap();
    let inserts = insert_requests(&requests);
    assert_eq!(inserts.len(), 2);
    assert_eq!(
        String::from_utf8_lossy(&inserts[0].body),
        String::from_utf8_lossy(&inserts[1].body),
        "the retry must send the same rows"
    );
}

#[tokio::test]
async fn a_permanent_insert_error_stops_the_process() {
    let server = MockServer::start().await;
    schema_mock(&server, MODERN_COLUMNS).await;
    Mock::given(method("POST"))
        .and(body_string_contains("sensor.rejected"))
        .respond_with(ResponseTemplate::new(401))
        .with_priority(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .with_priority(5)
        .mount(&server)
        .await;

    let (ws_url, _done) = spawn_fake_ha(
        vec![event("sensor.rejected", json!("1"), "2026-10-11T16:10:01Z")],
        false,
    )
    .await;
    let mut config = test_config(ws_url, Url::parse(&server.uri()).unwrap());
    config.batch.size = 1;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run(config, std::future::pending::<()>()),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            result,
            Err(Error::ClickHouse { status, .. }) if status == reqwest::StatusCode::UNAUTHORIZED
        ),
        "got {result:?}"
    );
}

#[tokio::test]
async fn drains_the_buffer_on_shutdown() {
    let server = clickhouse_mock().await;
    let events = vec![
        event("sensor.drain1", json!("1"), "2026-10-11T16:10:01Z"),
        event("sensor.drain2", json!("2"), "2026-10-11T16:10:02Z"),
        event("sensor.drain3", json!("3"), "2026-10-11T16:10:03Z"),
    ];
    let (ws_url, done) = spawn_fake_ha(events, false).await;
    let mut config = test_config(ws_url, Url::parse(&server.uri()).unwrap());
    config.batch.size = 10; // only the shutdown drain can flush these rows
    let (notify, handle) = spawn_ingester(config);

    tokio::time::timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap();
    // Give the reader a moment to consume the frames that are already buffered.
    tokio::time::sleep(Duration::from_millis(500)).await;

    notify.notify_one();
    let stats = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(stats.inserted, 3);

    let requests = server.received_requests().await.unwrap();
    let inserts = insert_requests(&requests);
    assert_eq!(inserts.len(), 1);
    assert_eq!(String::from_utf8_lossy(&inserts[0].body).lines().count(), 3);
}

#[tokio::test]
async fn a_legacy_table_is_rejected() {
    let server = MockServer::start().await;
    schema_mock(&server, LEGACY_COLUMNS).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .with_priority(5)
        .mount(&server)
        .await;

    let mut config = test_config(
        Url::parse("ws://127.0.0.1:1/api/websocket").unwrap(),
        Url::parse(&server.uri()).unwrap(),
    );
    config.create_schema = false;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run(config, std::future::pending::<()>()),
    )
    .await
    .unwrap();
    assert!(
        matches!(result, Err(Error::Schema(ref message)) if message.contains("time_fired")),
        "got {result:?}"
    );
}
