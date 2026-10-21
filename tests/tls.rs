//! Exercise the real binary over `wss://` with a locally generated CA.
//!
//! The child process gets `SSL_CERT_FILE` pointing at the test CA, so this
//! covers the same `rustls-native-certs` path used in production. The test is
//! compiled out when the `tls` feature is disabled.

#![cfg(feature = "tls")]

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn connects_over_wss_with_a_custom_ca() {
    // A throwaway CA and a server certificate for 127.0.0.1.
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem();
    let issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().unwrap();
    let server_params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
    let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

    let tls_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![server_cert.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(tls_config));

    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let tls = acceptor.accept(tcp).await.unwrap();
        let mut ws = accept_async(tls).await.unwrap();

        ws.send(Message::Text(
            r#"{"type":"auth_required","ha_version":"test"}"#.into(),
        ))
        .await
        .unwrap();
        let auth = next_json(&mut ws).await;
        assert_eq!(auth["type"], "auth");
        assert_eq!(auth["access_token"], "test-token");

        ws.send(Message::Text(
            r#"{"type":"auth_ok","ha_version":"test"}"#.into(),
        ))
        .await
        .unwrap();
        let subscribe = next_json(&mut ws).await;
        assert_eq!(subscribe["type"], "subscribe_events");
        ws.send(Message::Text(
            json!({"id": subscribe["id"], "type": "result", "success": true, "result": null})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

        let event = json!({
            "id": 1,
            "type": "event",
            "event": {
                "event_type": "state_changed",
                "data": {
                    "entity_id": "sensor.tls",
                    "old_state": {},
                    "new_state": {
                        "entity_id": "sensor.tls",
                        "state": "1",
                        "attributes": {},
                        "last_changed": "2026-10-11T16:10:02+00:00",
                        "last_updated": "2026-10-11T16:10:02+00:00"
                    }
                },
                "time_fired": "2026-10-11T16:10:02+00:00"
            }
        });
        ws.send(Message::Text(event.to_string().into()))
            .await
            .unwrap();

        // Keep the socket open until the ingester disconnects.
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

    let ca_path =
        std::env::temp_dir().join(format!("ha-ch-ingestor-test-ca-{}.pem", std::process::id()));
    std::fs::write(&ca_path, ca_pem).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_ha-ch-ingestor"))
        .arg("--dry-run")
        .env("HA_WS_URL", format!("wss://127.0.0.1:{port}/api/websocket"))
        .env("HA_TOKEN", "test-token")
        .env("SSL_CERT_FILE", &ca_path)
        .env("BATCH_SIZE", "1")
        .env("FLUSH_INTERVAL", "1")
        .env("RUST_LOG", "warn")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let line = tokio::time::timeout(Duration::from_secs(15), lines.next_line())
        .await
        .expect("timed out waiting for a row over wss")
        .unwrap()
        .expect("the child exited without writing a row");

    let row: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(row["entity_id"], "sensor.tls");
    assert_eq!(row["state_float"], 1.0);

    let _ = child.kill().await;
    let _ = std::fs::remove_file(ca_path);
}

async fn next_json<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
            Message::Binary(bytes) => return serde_json::from_slice(&bytes).unwrap(),
            _ => continue,
        }
    }
}
