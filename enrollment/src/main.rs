//! Serves the enrollment interface.

use std::{net::SocketAddr, sync::Arc};

use enrollment::{AppState, Store, router};

/// Where the CA that signs approved requests is read from.
const CA_CERT_PATH: &str = "ENROLLMENT_CA_CERT";
/// The CA private key.
const CA_KEY_PATH: &str = "ENROLLMENT_CA_KEY";
/// Address to listen on.
const LISTEN_ADDR: &str = "ENROLLMENT_LISTEN_ADDR";
/// Common name recorded for the CA when loading it.
const CA_COMMON_NAME: &str = "ENROLLMENT_CA_COMMON_NAME";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let ca_cert_path = std::env::var(CA_CERT_PATH)?;
    let ca_key_path = std::env::var(CA_KEY_PATH)?;
    let common_name = std::env::var(CA_COMMON_NAME).unwrap_or_else(|_unset| "grid-ca".to_owned());
    let listen = std::env::var(LISTEN_ADDR).unwrap_or_else(|_unset| "0.0.0.0:8080".to_owned());

    let ca = certs::load_ca(
        &common_name,
        &std::fs::read_to_string(&ca_key_path)?,
        &std::fs::read_to_string(&ca_cert_path)?,
    )?;

    let state = Arc::new(AppState {
        store: Store::memory(),
        ca,
    });

    let addr: SocketAddr = listen.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "enrollment service listening");

    axum::serve(listener, router(state)).await?;
    Ok(())
}
