//! Serves the enrollment interface.

use std::{net::SocketAddr, sync::Arc};

use enrollment::{AppState, Operators, Store, router};

/// Where the CA that signs approved requests is read from.
const CA_CERT_PATH: &str = "ENROLLMENT_CA_CERT";
/// The CA private key.
const CA_KEY_PATH: &str = "ENROLLMENT_CA_KEY";
/// Address to listen on.
const LISTEN_ADDR: &str = "ENROLLMENT_LISTEN_ADDR";
/// Common name recorded for the CA when loading it.
const CA_COMMON_NAME: &str = "ENROLLMENT_CA_COMMON_NAME";
/// Table of operators allowed to decide on requests.
const OPERATOR_TOKENS: &str = "ENROLLMENT_OPERATOR_TOKENS";

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
        operators: load_operators()?,
    });

    let addr: SocketAddr = listen.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "enrollment service listening");

    axum::serve(listener, router(state)).await?;
    Ok(())
}

/// Read the operator token table.
///
/// No table means nobody can approve, rather than anybody.
fn load_operators() -> Result<Operators, std::io::Error> {
    let operators = match std::env::var(OPERATOR_TOKENS) {
        Ok(path) => Operators::from_table(&std::fs::read_to_string(&path)?),
        Err(_unset) => Operators::default(),
    };

    if operators.is_empty() {
        tracing::warn!(
            "no operator tokens configured, so no request can be approved: set {OPERATOR_TOKENS} to a file of name:token lines"
        );
    } else {
        tracing::info!(operators = operators.len(), "operator tokens loaded");
    }
    Ok(operators)
}
