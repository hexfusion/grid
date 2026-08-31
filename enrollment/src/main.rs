//! Serves the enrollment interface.

use std::{net::SocketAddr, sync::Arc};

use enrollment::{AppState, JoiningConfig, Operators, Store, authz::Authorizer, router};

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
/// How many seconds an issued certificate lasts.
const CERT_LIFETIME_SECS: &str = "ENROLLMENT_CERT_LIFETIME_SECS";
/// Shared gossip transport key, base64, handed to a member on joining.
const GOSSIP_KEY: &str = "ENROLLMENT_GOSSIP_KEY";
/// Comma-separated peers a joining member announces to.
const GOSSIP_SEEDS: &str = "ENROLLMENT_GOSSIP_SEEDS";
/// Postgres connection URL.
///
/// Named to match MaaS, which carries it under this key in the `maas-db-config`
/// secret, so a deployment beside MaaS points at the database already there.
const DB_CONNECTION_URL: &str = "DB_CONNECTION_URL";

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
        store: open_store().await?,
        ca,
        authorizer: build_authorizer().await?,
        cert_lifetime: load_cert_lifetime(),
        joining: load_joining_config(),
    });

    let addr: SocketAddr = listen.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "enrollment service listening");

    axum::serve(listener, router(state)).await?;
    Ok(())
}

/// Read what every joining member is handed besides its certificate.
///
/// Without a gossip key a member holds a valid identity and still cannot reach
/// the mesh, which looks like enrollment working and nothing happening, so this
/// says which case the deployment is in.
fn load_joining_config() -> JoiningConfig {
    let gossip_key = std::env::var(GOSSIP_KEY).ok().filter(|key| !key.is_empty());
    let seeds: Vec<String> = std::env::var(GOSSIP_SEEDS)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|seed| !seed.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    if gossip_key.is_none() {
        tracing::warn!(
            "{GOSSIP_KEY} is not set, so an admitted member receives no gossip key and cannot join the mesh"
        );
    }
    if seeds.is_empty() {
        tracing::warn!("{GOSSIP_SEEDS} is not set, so an admitted member is told of no peers to announce to");
    }

    JoiningConfig { gossip_key, seeds }
}

/// Open the store the enrollment record lives in.
///
/// Falls back to keeping requests in this process, which loses them on restart
/// and shares nothing between replicas. That suits a local trial and nothing
/// else, so it says so.
async fn open_store() -> Result<Store, Box<dyn std::error::Error>> {
    match std::env::var(DB_CONNECTION_URL) {
        Ok(url) => {
            let store = Store::postgres(&url).await?;
            tracing::info!("enrollment records are kept in Postgres");
            Ok(store)
        },
        Err(_unset) => {
            tracing::warn!(
                "{DB_CONNECTION_URL} is not set, so enrollment records are kept in memory and lost on restart"
            );
            Ok(Store::memory())
        },
    }
}

/// Read how long issued certificates should last.
///
/// Expiry is the only thing that removes a member, so this is what bounds how
/// long a decision to admit someone stays in force.
fn load_cert_lifetime() -> time::Duration {
    let configured = std::env::var(CERT_LIFETIME_SECS)
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|secs| *secs > 0)
        .map(time::Duration::seconds);

    let lifetime = configured.unwrap_or(certs::DEFAULT_SITE_CERT_LIFETIME);
    tracing::info!(
        seconds = lifetime.whole_seconds(),
        "issued certificates expire after this"
    );
    lifetime
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

/// Build the operator-authorization backend.
///
/// Defaults to the operator-token table. Built with `--features sar` and
/// `ENROLLMENT_AUTHZ=kube`, it reuses Kubernetes RBAC (`TokenReview` +
/// `SubjectAccessReview`) instead — the `FlightCtl` pattern. `gridctl` is
/// unchanged either way; only the token's origin and who decides differ.
#[cfg_attr(
    not(feature = "sar"),
    expect(clippy::unused_async, reason = "async only when the sar backend is built")
)]
async fn build_authorizer() -> Result<Authorizer, Box<dyn std::error::Error>> {
    // Read the choice unconditionally and fail closed: an operator who asks for a
    // backend this binary cannot provide (`kube` without `--features sar`, or an
    // unrecognized value) must not silently fall back to the token table while
    // believing something stronger is deciding.
    match std::env::var("ENROLLMENT_AUTHZ").ok().as_deref() {
        None | Some("" | "local") => {
            tracing::info!("operator authorization: operator-token table");
            Ok(Authorizer::Local(load_operators()?))
        },
        Some("kube") => {
            #[cfg(feature = "sar")]
            {
                let kube = enrollment::authz::KubeAuthorizer::connect()
                    .await
                    .map_err(std::io::Error::other)?;
                tracing::info!("operator authorization: Kubernetes RBAC (SubjectAccessReview)");
                Ok(Authorizer::Kube(kube))
            }
            #[cfg(not(feature = "sar"))]
            Err("ENROLLMENT_AUTHZ=kube requires a build with --features sar".into())
        },
        Some(other) => Err(format!("unknown ENROLLMENT_AUTHZ={other:?}; expected \"local\" or \"kube\"").into()),
    }
}
