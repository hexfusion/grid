//! AI Grid operator binary.
//!
//! Runs Kubernetes controllers for [`GridNetwork`], [`GridSite`], and
//! [`InferenceProvider`] resources, and optionally starts a live SWIM
//! membership runtime for peer-to-peer mesh formation.
//!
//! # SWIM configuration
//!
//! Set `GRID_SWIM_BIND_ADDR` (e.g. `"0.0.0.0:7946"`) to enable the SWIM
//! runtime. Set `GRID_SWIM_ADVERTISE_ADDR` when the bind address is not
//! directly reachable by peers, and set `GRID_SWIM_SEEDS` to a comma-separated
//! list of seed peer socket addresses. When `GRID_SWIM_BIND_ADDR` is absent
//! the operator runs in static mode (`membership = None`);
//! `GridNetwork.status.connectedSites` and `distributedProviderCount` remain
//! 0, and the phase stays `Pending`/`Initializing` based on TLS configuration
//! only.
//!
//! # SWIM encryption (environment variable)
//!
//! Set `GRID_SWIM_ENCRYPT_KEY` to a 64-character lowercase hex string (32 bytes)
//! to enable AES-256-GCM encryption for all SWIM gossip packets.  When set,
//! packets from peers without the same key are silently dropped.
//!
//! This is the environment-variable path, intended for local development and
//! Kind-based testing.  Environment variables are visible to same-host process
//! inspectors, so the production configuration path uses
//! `GridNetwork.spec.tls.swimKeyRef` to source the key from a Kubernetes
//! Secret; the `GridNetwork` controller loads it and calls
//! `SwimHandle::set_swim_key` at reconcile time.
//!
//! The key value is **never** written to logs or tracing spans.
//!
//! [`GridNetwork`]: operator::crd::grid_network::GridNetwork
//! [`GridSite`]: operator::crd::grid_site::GridSite
//! [`InferenceProvider`]: operator::crd::inference_provider::InferenceProvider

#![deny(unsafe_code)]
#![expect(
    clippy::arithmetic_side_effects,
    clippy::min_ident_chars,
    reason = "operator uses short closure params and index arithmetic pervasively"
)]

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::response::IntoResponse as _;
use clap::Parser as _;
use futures::StreamExt as _;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Api, Client,
    api::{ObjectMeta, PostParams},
    runtime::{controller::Controller, watcher},
};
use operator::{
    cli::Cli,
    controller::{
        grid_network::{self, OperatorCtx},
        grid_site, inference_provider,
    },
    crd::{grid_network::GridNetwork, grid_site::GridSite, inference_provider::InferenceProvider},
    gateway,
    swim_runtime::{self, RevisionLease, SwimConfig},
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
#[expect(
    clippy::large_stack_frames,
    clippy::too_many_lines,
    reason = "top-level binary with tokio runtime; the startup sequence reads better whole"
)]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("starting grid-operator");

    let config = Cli::parse();

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to create kube client");
            return;
        },
    };

    let swim = maybe_start_swim(&client, &config.gateway).await;

    if let Some(handle) = &swim {
        tokio::spawn(gateway::run_discovery_poller(
            client.clone(),
            Arc::clone(handle),
            config.gateway.clone(),
        ));
    }

    let swim_for_poller = swim.clone();
    let ctx = Arc::new(OperatorCtx::new(client.clone(), swim));

    // Dropping it also triggers, so an unwind still stands the pollers down.
    let (trigger, shutdown) = operator::shutdown::Trigger::new();
    tokio::spawn(watch_for_termination(trigger));

    let result = tokio::try_join!(
        run_network_controller(client.clone(), Arc::clone(&ctx)),
        run_site_controller(client.clone()),
        run_provider_controller(client.clone()),
        run_metrics_server(),
        run_signals_server(
            client.clone(),
            ctx.signals(),
            ctx.peers(),
            Arc::new(grid_network::local_site_labels().unwrap_or_default()),
            ctx.peer_identities(),
        ),
        run_peer_poller(Arc::clone(&ctx), swim_for_poller, client.clone(), shutdown.clone()),
        run_local_scraper(Arc::clone(&ctx), client.clone()),
    );

    if let Err(e) = result {
        tracing::error!(error = %e, "controller error");
    }
}

/// Trigger shutdown on the signals a container runtime actually sends.
///
/// SIGTERM is what Kubernetes sends before it waits out the grace period, so it
/// is the one that matters; SIGINT is here so a run in a terminal behaves the
/// same way. Losing the handler is not fatal: the process still stops, it just
/// stops without letting anything stand down first, and saying so beats
/// pretending shutdown is graceful when it is not.
async fn watch_for_termination(trigger: operator::shutdown::Trigger) {
    let signal = first_termination_signal().await;
    tracing::info!(signal, "standing down");
    trigger.trigger();
}

/// Wait for whichever termination signal arrives first, and name it.
///
/// SIGTERM is what a container runtime sends before it waits out the grace
/// period, so it is the one that matters. SIGINT is here so a run in a terminal
/// behaves the same way.
async fn first_termination_signal() -> &'static str {
    let Ok(mut term) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) else {
        tracing::warn!("cannot watch for SIGTERM; only an interrupt will stand down cleanly");
        drop(tokio::signal::ctrl_c().await);
        return "SIGINT";
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = tokio::signal::ctrl_c() => "SIGINT",
    }
}

// ---------------------------------------------------------------------------
// Hostname helper
// ---------------------------------------------------------------------------

/// Optionally start the SWIM runtime from environment variables.
///
/// Returns `Some(handle)` if `GRID_SWIM_BIND_ADDR` is set and the runtime
/// starts successfully.  Returns `None` when the variable is absent,
/// unparseable, or the bind fails (all logged at error level).
///
/// Gateway address resolution uses [`operator::gateway::resolve`]:
/// `GRID_GATEWAY_ADDRESS` env var wins; otherwise the operator discovers
/// its own provider gateway Service `LoadBalancer` IP from Kubernetes.
#[expect(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::large_stack_frames,
    reason = "sequential env-var parsing + runtime startup; splitting would obscure the startup sequence"
)]
async fn maybe_start_swim(client: &Client, config: &gateway::Config) -> Option<Arc<swim_runtime::SwimHandle>> {
    let addr_str = std::env::var("GRID_SWIM_BIND_ADDR").ok()?;
    let bind_addr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(addr = %addr_str, error = %e, "GRID_SWIM_BIND_ADDR not a valid socket address");
            return None;
        },
    };
    let advertise_addr = parse_optional_socket_addr_env("GRID_SWIM_ADVERTISE_ADDR");
    let seeds = parse_socket_addr_list_env("GRID_SWIM_SEEDS");
    let site_name = std::env::var("GRID_SWIM_SITE_NAME").unwrap_or_else(|_| hostname_or_default());
    let gateway_address = match gateway::resolve(client, config).await {
        Ok(addr) => addr,
        Err(e) => {
            tracing::error!(error = %e, "gateway address discovery failed; continuing without");
            None
        },
    };
    let swim_key = parse_swim_key_env("GRID_SWIM_ENCRYPT_KEY");
    let revision_lease = match reserve_revision_lease(client, &site_name).await {
        Ok(lease) => lease,
        Err(error) => {
            tracing::error!(%error, "failed to reserve SWIM revisions; running in static mode");
            return None;
        },
    };
    let cfg = SwimConfig {
        bind_addr,
        advertise_addr,
        site_name: site_name.clone(),
        seeds,
        gateway_address,
        swim_key,
        identity: load_grid_identity(),
        revision_lease,
    };
    match swim_runtime::start(cfg).await {
        Ok(handle) => {
            tracing::info!(addr = %addr_str, "SWIM runtime started");
            Some(handle)
        },
        Err(e) => {
            tracing::error!(error = %e, "SWIM runtime failed to start; running in static mode");
            None
        },
    }
}

/// Number of revisions reserved durably for each operator process.
///
/// At the current one-second metadata repair rate this covers more than a
/// century. Exhaustion still causes the runtime to stop rather than reuse a
/// published revision.
const REVISION_LEASE_SIZE: u64 = 1_u64 << 32;
/// Maximum in-process foca identity renewals reserved for one operator.
const NODE_GENERATION_LEASE_SIZE: u64 = 1_u64 << 20;
/// Maximum resource-version conflicts retried during one reservation.
const REVISION_RESERVATION_ATTEMPTS: usize = 8;
/// `ConfigMap` data key containing the last reserved transport revision.
const REVISION_HIGH_KEY: &str = "revisionHighWatermark";
/// `ConfigMap` data key containing the last reserved identity generation.
const NODE_GENERATION_HIGH_KEY: &str = "nodeGenerationHighWatermark";

/// Reserve a disjoint transport-revision range and node generation.
///
/// The upper bound is written before any revision in the range can be
/// published. `replace` includes the `ConfigMap`'s `resourceVersion`, so
/// concurrent operator starts conflict and retry instead of overwriting one
/// another.
#[expect(
    clippy::too_many_lines,
    clippy::large_stack_frames,
    reason = "the Kubernetes read/create/replace CAS loop keeps each conflict and fail-closed path explicit"
)]
async fn reserve_revision_lease(client: &Client, site_name: &str) -> Result<RevisionLease, String> {
    let api: Api<ConfigMap> = Api::default_namespaced(client.clone());
    let cm_name = format!("grid-swim-revision-hwm-{site_name}");
    for _attempt in 0..REVISION_RESERVATION_ATTEMPTS {
        match api.get(&cm_name).await {
            Ok(mut cm) => {
                let data = cm.data.as_ref().ok_or_else(|| format!("{cm_name} has no data"))?;
                let current_high = parse_revision_value(data, REVISION_HIGH_KEY)
                    .or_else(|| parse_revision_value(data, "revision"))
                    .ok_or_else(|| format!("{cm_name} has no valid revision high-water mark"))?;
                let current_generation_high = parse_revision_value(data, NODE_GENERATION_HIGH_KEY)
                    .or_else(|| parse_revision_value(data, "nodeGeneration"))
                    .unwrap_or(0);
                let lease = next_revision_lease(current_high, current_generation_high)?;
                cm.data = Some(revision_lease_data(&lease));
                match api.replace(&cm_name, &PostParams::default(), &cm).await {
                    Ok(_) => {
                        tracing::info!(
                            first_revision = lease.first_revision,
                            last_revision = lease.last_revision,
                            first_node_generation = lease.first_node_generation,
                            last_node_generation = lease.last_node_generation,
                            cm = %cm_name,
                            "reserved SWIM revision range"
                        );
                        return Ok(lease);
                    },
                    Err(kube::Error::Api(conflict)) if conflict.code == 409 => {},
                    Err(replace_err) => return Err(format!("replace {cm_name}: {replace_err}")),
                }
            },
            Err(kube::Error::Api(not_found)) if not_found.code == 404 => {
                let lease = initial_revision_lease()?;
                let cm = ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(cm_name.clone()),
                        ..ObjectMeta::default()
                    },
                    data: Some(revision_lease_data(&lease)),
                    ..ConfigMap::default()
                };
                match api.create(&PostParams::default(), &cm).await {
                    Ok(_) => {
                        tracing::info!(
                            first_revision = lease.first_revision,
                            last_revision = lease.last_revision,
                            first_node_generation = lease.first_node_generation,
                            last_node_generation = lease.last_node_generation,
                            cm = %cm_name,
                            "created SWIM revision reservation"
                        );
                        return Ok(lease);
                    },
                    Err(kube::Error::Api(conflict)) if conflict.code == 409 => {},
                    Err(create_err) => return Err(format!("create {cm_name}: {create_err}")),
                }
            },
            Err(read_err) => return Err(format!("read {cm_name}: {read_err}")),
        }
    }
    Err(format!(
        "could not reserve SWIM revisions in {cm_name} after {REVISION_RESERVATION_ATTEMPTS} conflicts"
    ))
}

/// Parse an unsigned value from `ConfigMap` data.
fn parse_revision_value(data: &BTreeMap<String, String>, key: &str) -> Option<u64> {
    data.get(key).and_then(|value| value.parse().ok())
}

/// Render the durable high-water marks for one reservation.
fn revision_lease_data(lease: &RevisionLease) -> BTreeMap<String, String> {
    BTreeMap::from([
        (REVISION_HIGH_KEY.to_owned(), lease.last_revision.to_string()),
        (
            NODE_GENERATION_HIGH_KEY.to_owned(),
            lease.last_node_generation.to_string(),
        ),
    ])
}

/// Build the first durable lease from wall-clock seeds.
fn initial_revision_lease() -> Result<RevisionLease, String> {
    let revision_seed = unix_millis()?;
    let node_generation = unix_nanos()?;
    lease_from_seeds(revision_seed, node_generation)
}

/// Build the next lease strictly after persisted high-water marks.
fn next_revision_lease(current_high: u64, current_generation_high: u64) -> Result<RevisionLease, String> {
    let revision_seed = current_high
        .checked_add(1)
        .ok_or_else(|| "SWIM revision high-water mark exhausted".to_owned())?
        .max(unix_millis()?);
    let node_generation = current_generation_high
        .checked_add(1)
        .ok_or_else(|| "SWIM node generation exhausted".to_owned())?
        .max(unix_nanos()?);
    lease_from_seeds(revision_seed, node_generation)
}

/// Build bounded revision and generation ranges from inclusive first values.
fn lease_from_seeds(first_revision: u64, node_generation: u64) -> Result<RevisionLease, String> {
    let last_revision = first_revision
        .checked_add(REVISION_LEASE_SIZE - 1)
        .ok_or_else(|| "SWIM revision range exhausted".to_owned())?;
    let last_node_generation = node_generation
        .checked_add(NODE_GENERATION_LEASE_SIZE - 1)
        .ok_or_else(|| "SWIM node generation range exhausted".to_owned())?;
    Ok(RevisionLease {
        first_revision,
        last_revision,
        first_node_generation: node_generation,
        last_node_generation,
    })
}

/// Return milliseconds since the Unix epoch as `u64`.
fn unix_millis() -> Result<u64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before Unix epoch: {error}"))?
        .as_millis();
    u64::try_from(millis).map_err(|_error| "Unix millisecond value exceeds u64".to_owned())
}

/// Return nanoseconds since the Unix epoch as `u64`.
fn unix_nanos() -> Result<u64, String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before Unix epoch: {error}"))?
        .as_nanos();
    u64::try_from(nanos).map_err(|_error| "Unix nanosecond value exceeds u64".to_owned())
}

/// Parse `GRID_SWIM_ENCRYPT_KEY` as a 32-byte AES-256-GCM key from 64 hex characters.
///
/// Returns `None` when the env var is absent (no encryption).
/// Logs an error and returns `None` when the value is present but malformed.
///
/// # Security invariant
///
/// The decoded key bytes are never written to logs or tracing spans.
fn parse_swim_key_env(name: &str) -> Option<swim::crypto::SwimKey> {
    let hex = std::env::var(name).ok()?;
    let hex = hex.trim();
    if hex.len() != 64 {
        tracing::error!(
            env = name,
            len = hex.len(),
            "SWIM encryption key must be a 64-character hex string (32 bytes); ignoring"
        );
        return None;
    }
    // Parse hex byte-by-byte using char::to_digit to avoid string slice indexing.
    // to_digit(16) returns 0..=15 as u32; cast to u8 is safe and done immediately.
    let hex_nibbles: Vec<u8> = hex
        .chars()
        .filter_map(|c| c.to_digit(16).and_then(|n| u8::try_from(n).ok()))
        .collect();
    if hex_nibbles.len() != 64 {
        tracing::error!(
            env = name,
            "SWIM encryption key contains invalid hex character; ignoring"
        );
        return None;
    }
    let mut key = [0_u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        let hi = hex_nibbles.get(i * 2).copied().unwrap_or(0);
        let lo = hex_nibbles.get(i * 2 + 1).copied().unwrap_or(0);
        *byte = (hi << 4) | lo;
    }
    tracing::info!(env = name, "SWIM encryption key loaded from environment");
    Some(key)
}

/// Parse an optional socket address environment variable.
fn parse_optional_socket_addr_env(name: &str) -> Option<SocketAddr> {
    let value = std::env::var(name).ok()?;
    match value.parse() {
        Ok(addr) => Some(addr),
        Err(e) => {
            tracing::error!(env = name, value = %value, error = %e, "SWIM socket address env var is invalid");
            None
        },
    }
}

/// Parse a comma-separated socket address environment variable.
fn parse_socket_addr_list_env(name: &str) -> Vec<SocketAddr> {
    let Ok(value) = std::env::var(name) else {
        return Vec::new();
    };

    value
        .split(',')
        .filter_map(|raw| {
            let item = raw.trim();
            if item.is_empty() {
                return None;
            }
            match item.parse() {
                Ok(addr) => Some(addr),
                Err(e) => {
                    tracing::error!(env = name, value = %item, error = %e, "SWIM seed address is invalid");
                    None
                },
            }
        })
        .collect()
}

/// Return the machine hostname or a safe fallback.
fn hostname_or_default() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "grid-operator".to_owned())
}

// ---------------------------------------------------------------------------
// Controller Setup
// ---------------------------------------------------------------------------

/// Run the [`GridNetwork`] controller.
///
/// In addition to watching `GridNetwork` resources, this controller watches
/// `InferenceProvider`, `GridSite`, and `Secret` resources.  Secret changes
/// trigger reconciliation of affected `GridNetwork`s when providers change.
///
/// Metrics TLS rotation is detected by bounded requeue rather than a
/// cluster-wide Secret watch — the operator only reads referenced
/// Secrets by explicit namespace/name during reconciliation.
#[expect(
    clippy::too_many_lines,
    reason = "controller setup with two cross-resource watches and optional SWIM"
)]
async fn run_network_controller(
    client: Client,
    ctx: Arc<OperatorCtx>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridNetwork>::all(client.clone());
    let provider_api = Api::<InferenceProvider>::all(client.clone());
    let site_api = Api::<GridSite>::all(client.clone());

    let controller = Controller::new(api, watcher::Config::default())
        .watches(
            provider_api,
            watcher::Config::default(),
            grid_network::network_refs_from_inference_provider,
        )
        .watches(
            site_api,
            watcher::Config::default(),
            grid_network::network_refs_from_grid_site,
        );
    let controller = if let Some(swim) = ctx.swim.as_ref() {
        controller.reconcile_all_on(swim.reconciliation_events())
    } else {
        controller
    };
    controller
        .run(grid_network::reconcile, grid_network::error_policy, ctx)
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled GridNetwork"),
                Err(e) => tracing::error!(error = ?e, "GridNetwork watch error"),
            }
        })
        .await;

    Ok(())
}

/// Run the [`GridSite`] controller.
async fn run_site_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<GridSite>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .with_config(kube::runtime::controller::Config::default().concurrency(16))
        .run(grid_site::reconcile, grid_site::error_policy, Arc::new(client))
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled GridSite"),
                Err(e) => tracing::error!(error = ?e, "GridSite watch error"),
            }
        })
        .await;

    Ok(())
}

/// Run the [`InferenceProvider`] controller (OP-02).
///
/// Watches `InferenceProvider` resources.  Metrics TLS rotation is detected
/// by bounded requeue rather than a cluster-wide Secret watch — the operator
/// only reads referenced Secrets by explicit namespace/name during
/// reconciliation.
///
/// [`InferenceProvider`]: operator::crd::inference_provider::InferenceProvider
async fn run_provider_controller(client: Client) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Api::<InferenceProvider>::all(client.clone());

    Controller::new(api, watcher::Config::default())
        .run(
            inference_provider::reconcile,
            inference_provider::error_policy,
            Arc::new(client),
        )
        .for_each(|result| async {
            match result {
                Ok((obj, _action)) => tracing::info!(%obj, "reconciled InferenceProvider"),
                Err(e) => tracing::error!(error = ?e, "InferenceProvider watch error"),
            }
        })
        .await;

    Ok(())
}

/// Serve Prometheus metrics and health endpoints.
async fn run_metrics_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("GRID_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_owned());
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/healthz", axum::routing::get(health_handler))
        .route("/readyz", axum::routing::get(health_handler));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound_addr = listener.local_addr().map_or_else(|_| addr.clone(), |a| a.to_string());
    tracing::info!(addr = %bound_addr, "metrics server started");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Serve this site's scraped provider signals for its gateway to read.
///
/// Separate from the operator's own metrics server because the two have
/// different audiences: `/metrics` is scraped by the cluster's monitoring, and
/// this is read by the gateway at a much shorter interval. Keeping them apart
/// leaves room to require mutual TLS here without touching monitoring.
/// How often the listener re-reads its own certificate.
///
/// Material rarely arrives with the process. cert-manager writes the Secret
/// when it gets around to it, which is routinely after the operator rolls, and
/// it rewrites it on every renewal. Resolving once at startup meant a listener
/// that came up before its certificate stayed plaintext for the process
/// lifetime, and one that came up after a renewal kept serving the old key.
const SIGNALS_TLS_POLL: std::time::Duration = std::time::Duration::from_secs(30);

/// Serve provider signals, rebinding whenever the material changes.
///
/// Separate from the operator's own metrics server because the two have
/// different audiences: `/metrics` is scraped by the cluster's monitoring, and
/// this is read by the gateway at a much shorter interval.
async fn run_signals_server(
    client: Client,
    site: operator::signals::SignalStore,
    peers: operator::signals::SignalStore,
    local_labels: Arc<BTreeMap<String, String>>,
    peer_identities: operator::signals::PeerIdentities,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("GRID_SIGNALS_ADDR").unwrap_or_else(|_| "0.0.0.0:9091".to_owned());
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(signals_handler))
        .with_state(Published {
            site,
            peers,
            local_labels,
        });

    // rustls fixes the verifier at build time, so new material means serving
    // again, not swapping a field.
    loop {
        let (tls, own_key) = Box::pin(signals_identity(&client)).await;
        let identity = SignalsIdentity {
            peers: peer_identities.clone(),
            own_key: own_key.clone(),
        };
        serve_once(
            &addr,
            app.clone(),
            tls,
            identity,
            material_changed(client.clone(), own_key),
        )
        .await?;
        tracing::info!("signals TLS material changed; serving again");
    }
}

/// Bind and serve until `changed` resolves.
async fn serve_once(
    addr: &str,
    app: axum::Router,
    tls: Option<Arc<rustls::ServerConfig>>,
    identity: SignalsIdentity,
    changed: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener
        .local_addr()
        .map_or_else(|_| addr.to_owned(), |a| a.to_string());
    let Some(tls) = tls else {
        // No key presented means nobody can be named, so local scope only.
        tracing::warn!(
            addr = %bound,
            tls = false,
            "signals server started without TLS: every caller is served local scope"
        );
        let app = app.layer(axum::Extension(Caller::Local));
        axum::serve(listener, app).with_graceful_shutdown(changed).await?;
        return Ok(());
    };
    tracing::info!(addr = %bound, tls = true, "signals server started");
    serve_signals_tls(listener, app, tls, identity, changed).await
}

/// Resolves when this site's certificate stops matching `serving`.
async fn material_changed(client: Client, serving: Option<String>) {
    loop {
        tokio::time::sleep(SIGNALS_TLS_POLL).await;
        if Box::pin(signals_identity(&client)).await.1 != serving {
            return;
        }
    }
}

/// Accept signals connections, deciding scope from the certificate presented.
///
/// Client auth is optional, so the handshake tells a peer from a local
/// consumer: one presents a certificate the grid CA signed, the other does not.
/// Scope is fixed here, before any request parameter is read, so a caller
/// cannot widen what it receives by asking differently.
async fn serve_signals_tls(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    tls: Arc<rustls::ServerConfig>,
    identity: SignalsIdentity,
    changed: impl Future<Output = ()> + Send,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    let mut changed = std::pin::pin!(changed);
    loop {
        let accepted = tokio::select! {
            () = &mut changed => return Ok(()),
            accepted = listener.accept() => accepted,
        };
        let Ok((stream, remote)) = accepted else {
            continue;
        };
        tokio::spawn(serve_signals_connection(
            acceptor.clone(),
            app.clone(),
            stream,
            remote,
            identity.clone(),
        ));
    }
}

/// Who a connection speaks for, decided from the key it presented.
///
/// Every caller is named by a key. A peer holds one this site wrote down in a
/// `GridSite`; this site's own workloads hold the site certificate itself,
/// which the operator already loads to serve this listener and so can recognise
/// without anybody declaring it.
///
/// Nothing is trusted for presenting nothing. A caller with no certificate, or
/// one carrying a key nobody declared, is served nothing, so refusing a peer
/// cannot be undone by reconnecting without credentials.
fn caller_for(
    presented: Option<&[rustls::pki_types::CertificateDer<'static>]>,
    identity: &SignalsIdentity,
    remote: SocketAddr,
) -> Caller {
    let Some(leaf) = presented.and_then(<[_]>::first) else {
        tracing::debug!(%remote, "signals caller presented no certificate");
        return Caller::Peer(None);
    };
    let fingerprint = operator::signals::leaf_fingerprint(leaf);
    if identity.own_key.as_deref() == Some(fingerprint.as_str()) {
        return Caller::Local;
    }
    let named = identity.peers.resolve_by_key(&fingerprint);
    if named.is_none() {
        tracing::debug!(%remote, "signals caller presented a key this site has not declared");
    }
    Caller::Peer(named)
}

/// Handshake one connection, then serve it with the scope its certificate earns.
#[expect(clippy::large_stack_frames, reason = "async future over a rustls handshake")]
async fn serve_signals_connection(
    acceptor: tokio_rustls::TlsAcceptor,
    app: axum::Router,
    stream: tokio::net::TcpStream,
    remote: SocketAddr,
    identity: SignalsIdentity,
) {
    let Ok(stream) = acceptor.accept(stream).await else {
        tracing::debug!(%remote, "signals handshake failed");
        return;
    };
    let caller = caller_for(stream.get_ref().1.peer_certificates(), &identity, remote);
    let service = hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
        use tower::Service as _;
        let mut request = request;
        request.extensions_mut().insert(caller.clone());
        app.clone().call(request)
    });
    let io = hyper_util::rt::TokioIo::new(stream);
    if let Err(error) = hyper::server::conn::http1::Builder::new()
        .serve_connection(io, service)
        .await
    {
        tracing::debug!(%remote, %error, "signals connection ended");
    }
}

/// TLS material for the signals listener, or `None` to serve plaintext.
///
/// Read once. A network without configured TLS, or material that cannot be
/// read, leaves the listener plaintext rather than refusing to start, so a
/// grid that never wanted peer TLS is unaffected.
async fn signals_identity(client: &Client) -> (Option<Arc<rustls::ServerConfig>>, Option<String>) {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let network = networks
        .list(&kube::api::ListParams::default())
        .await
        .ok()
        .and_then(|list| list.items.into_iter().next());
    let Some(network) = network else {
        return (None, None);
    };
    (
        Box::pin(signals_listener_tls(&network, client)).await,
        Box::pin(signals_own_key(&network, client)).await,
    )
}

/// TLS for the listener, or `None` when nothing is configured.
async fn signals_listener_tls(network: &GridNetwork, client: &Client) -> Option<Arc<rustls::ServerConfig>> {
    match grid_network::signals_server_config(network, client).await {
        Ok(config) => {
            if config.is_none() {
                tracing::info!("signals TLS not configured; nobody can be named");
            }
            config
        },
        Err(error) => {
            tracing::warn!(%error, "signals TLS unavailable; serving plaintext");
            None
        },
    }
}

/// This site's own certificate fingerprint, for recognising its own workloads.
async fn signals_own_key(network: &GridNetwork, client: &Client) -> Option<String> {
    match grid_network::signals_own_key(network, client).await {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(%error, "this site's own certificate is unreadable; its workloads cannot be recognised");
            None
        },
    }
}

/// Scrape this site's providers on their own interval and publish the result.
///
/// Separate from reconcile, which runs on an interval sized for declarations
/// and is two orders of magnitude slower than these values move.
async fn run_local_scraper(
    ctx: Arc<OperatorCtx>,
    client: Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Milliseconds win: the seconds form cannot express a sub-second interval.
    let interval = scrape_interval();
    tracing::info!(interval_ms = interval.as_millis(), "local signals scraper started");
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    #[expect(
        clippy::infinite_loop,
        reason = "runs for the process lifetime alongside the controllers"
    )]
    loop {
        ticker.tick().await;
        let networks: Api<GridNetwork> = Api::all(client.clone());
        let Ok(list) = networks.list(&kube::api::ListParams::default()).await else {
            continue;
        };
        for network in list.items.iter().filter_map(|n| n.metadata.name.as_deref()) {
            if let Err(error) = grid_network::refresh_signals(&ctx, &client, network).await {
                tracing::warn!(network, %error, "peer signals refresh failed");
            }
        }
    }
}

/// Poll every alive peer's signals endpoint on a coarse interval.
///
/// Addresses come from SWIM membership, which already carries each site's
/// advertised address, so no field is added to a broadcast payload.
///
/// The interval is much longer than a gateway's read of its own operator: this
/// crosses an administrative boundary and carries another site's aggregate.
/// Build the peer poller and report the scheme it will use.
///
/// Split out so the poll loop stays short; the scheme follows the TLS material
/// because a client configured for TLS cannot speak to a plaintext peer.
async fn peer_source(
    client: &Client,
    shutdown: operator::shutdown::Shutdown,
) -> (Box<dyn operator::signals::PeerSignals>, &'static str) {
    let tls = peer_tls(client).await;
    let scheme = if tls.is_some() { "https" } else { "http" };
    let source: Box<dyn operator::signals::PeerSignals> = Box::new(operator::signals::PollPeers {
        timeout: std::time::Duration::from_secs(parse_env_or("GRID_SIGNALS_PEER_TIMEOUT_SECS", 5_u64)),
        tls,
        collect: parse_peer_collect(),
        concurrency: parse_env_or("GRID_SIGNALS_PEER_CONCURRENCY", 8_usize),
        attempts: parse_env_or("GRID_SIGNALS_PEER_ATTEMPTS", 3_u32),
        backoff: std::time::Duration::from_millis(parse_env_or("GRID_SIGNALS_PEER_BACKOFF_MS", 50_u64)),
        budget: std::time::Duration::from_secs(parse_env_or("GRID_SIGNALS_PEER_BUDGET_SECS", 10_u64)),
        slow_after: std::time::Duration::from_millis(parse_env_or("GRID_SIGNALS_PEER_SLOW_MS", 1_000_u64)),
        shutdown,
    });
    (source, scheme)
}

/// Poll every alive peer for its signals on a coarse interval.
///
/// Coarser than the local scrape because each request crosses a cluster
/// boundary, and a peer's values are only as fresh as its own collection.
async fn run_peer_poller(
    ctx: Arc<OperatorCtx>,
    swim: Option<Arc<swim_runtime::SwimHandle>>,
    client: Client,
    shutdown: operator::shutdown::Shutdown,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(swim) = swim else {
        tracing::info!("peer signals poller disabled: SWIM is not running");
        return Ok(());
    };
    let port = parse_env_or("GRID_SIGNALS_PEER_PORT", 9091_u16);
    let interval = std::time::Duration::from_secs(parse_env_or("GRID_SIGNALS_PEER_INTERVAL_SECS", 30_u64));
    // Rebuilt on change: a client resolved before its certificate existed would
    // speak plaintext for the process lifetime.
    while !shutdown.is_triggered() {
        let (source, scheme) = Box::pin(peer_source(&client, shutdown.clone())).await;
        tracing::info!(
            port,
            interval_secs = interval.as_secs(),
            source = source.name(),
            scheme,
            "peer signals poller started"
        );
        poll_until_material_changes(&ctx, &swim, source.as_ref(), port, scheme, interval, &shutdown, &client).await;
    }
    tracing::info!("peer signals poller stopped");
    Ok(())
}

/// Poll until the peer client's own material changes, or shutdown.
///
/// A client carries its trust decisions from when it was built, so picking up a
/// new certificate or a new CA means building again rather than swapping a
/// field, exactly as the listener rebinds.
#[expect(clippy::too_many_arguments, reason = "a poll loop needs its whole configuration")]
async fn poll_until_material_changes(
    ctx: &Arc<OperatorCtx>,
    swim: &Arc<swim_runtime::SwimHandle>,
    source: &dyn operator::signals::PeerSignals,
    port: u16,
    scheme: &str,
    interval: std::time::Duration,
    shutdown: &operator::shutdown::Shutdown,
    client: &Client,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = shutdown.triggered() => return,
            _ = ticker.tick() => {},
        }
        poll_peers_once(ctx, swim, source, port, scheme).await;
        if peer_scheme(client).await != scheme {
            tracing::info!("peer signals TLS material changed; rebuilding the client");
            return;
        }
    }
}

/// The scheme the peer client would use if built now.
async fn peer_scheme(client: &Client) -> &'static str {
    if peer_tls(client).await.is_some() {
        "https"
    } else {
        "http"
    }
}


/// Collect from every alive peer and replace what is published for them.
async fn poll_peers_once(
    ctx: &OperatorCtx,
    swim: &swim_runtime::SwimHandle,
    source: &dyn operator::signals::PeerSignals,
    port: u16,
    scheme: &str,
) {
    // A refusal is symmetric: one we will not answer is one we do not read.
    let identities = ctx.peer_identities();
    let snapshot = swim.snapshot();
    let alive = snapshot
        .members
        .iter()
        .filter(|m| m.status == operator::swim::MemberStatus::Alive)
        .filter(|m| !identities.refuses(&m.site_id))
        .map(|m| (m.site_id.as_str(), m.endpoint.as_str()));
    // Verified against the keys declared for that peer, not any key the CA signed.
    let mut sites = operator::signals::peer_sites(alive, swim.site_name(), port, scheme);
    for site in &mut sites {
        site.pins = identities.pins_for(&site.name);
    }
    if sites.is_empty() {
        return;
    }
    let collected = source.collect(&sites).await;
    ctx.peers().refresh(collected.into_iter().collect());
}

/// How often this site reads its own providers.
///
/// `GRID_SIGNALS_SCRAPE_INTERVAL_MS` wins when set, since the seconds form
/// bottoms out at one and this scrape never leaves the node. Floors at 50ms:
/// below that the operator spends more time scraping than the provider spends
/// changing, and every sample it publishes is the same one.
fn scrape_interval() -> std::time::Duration {
    let ms = parse_env_or(
        "GRID_SIGNALS_SCRAPE_INTERVAL_MS",
        parse_env_or("GRID_SIGNALS_SCRAPE_INTERVAL_SECS", 5_u64).saturating_mul(1_000),
    );
    std::time::Duration::from_millis(ms.max(50))
}


/// Client TLS for peer polling, or `None` when the network declares no trust.
///
/// Logged rather than fatal: a site with no TLS material still polls, and the
/// scheme in the poller's startup line says which mode it is in.
async fn peer_tls(client: &Client) -> Option<Arc<operator::signals::PeerTlsMaterial>> {
    let networks: Api<GridNetwork> = Api::all(client.clone());
    let list = networks.list(&kube::api::ListParams::default()).await.ok()?;
    let network = list.items.into_iter().next()?;
    match grid_network::peer_tls_config(&network, client).await {
        Ok(cfg) => cfg,
        Err(error) => {
            tracing::warn!(%error, "peer signals TLS unavailable; polling peers without it");
            None
        },
    }
}

/// Signals asked of each peer, from `GRID_SIGNALS_PEER_COLLECT`.
///
/// Newline-separated metric names. Empty asks a peer for everything it holds,
/// which is fine at a handful of providers and wasteful at many.
fn parse_peer_collect() -> Vec<String> {
    std::env::var("GRID_SIGNALS_PEER_COLLECT")
        .map(|raw| {
            raw.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Read an environment variable, falling back when unset or unparseable.
fn parse_env_or<T: std::str::FromStr + std::fmt::Display + Copy>(name: &str, fallback: T) -> T {
    match std::env::var(name) {
        Ok(raw) => raw.parse().unwrap_or_else(|_| {
            tracing::warn!(var = name, value = raw, default = %fallback, "unparseable; using default");
            fallback
        }),
        Err(_) => fallback,
    }
}

/// Who is asking, which decides what they are served.
///
/// A peer presented a certificate signed by the grid CA and speaks for another
/// site. Anything else is in-cluster and is served the whole grid.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Caller {
    /// Another site, named by the certificate it presented.
    ///
    /// Carries the labels this site holds for that peer, so a policy is matched
    /// against local record rather than against the peer's own claim.
    ///
    /// `None` when this site holds no record for the name, holds one that
    /// refuses reads, or holds pins the presented key does not match. A peer
    /// this site cannot place is served nothing at all: deleting its record is
    /// how a site is cut off, and an unrestricted target is unrestricted
    /// within this cluster rather than to the grid.
    Peer(Option<BTreeMap<String, String>>),
    /// A consumer inside this cluster.
    Local,
}

/// How a caller is named on the signals listener.
///
/// The two travel together everywhere: a peer is matched against what its
/// `GridSite` declares, and this site's own workloads against the certificate
/// the operator already serves with.
#[derive(Clone)]
struct SignalsIdentity {
    /// Keys peers have declared, and the labels held for each.
    peers: operator::signals::PeerIdentities,
    /// This site's own certificate fingerprint, when it has one.
    own_key: Option<String>,
}

/// Everything this operator publishes.
#[derive(Clone)]
struct Published {
    /// This site's own signals.
    site: operator::signals::SignalStore,
    /// What peers reported about themselves.
    peers: operator::signals::SignalStore,
    /// Labels a local consumer reads as, which are this site's own.
    ///
    /// A peer reads as itself, and naming it needs the identity in its
    /// certificate rather than this. Until that lands a peer is unnamed, so a
    /// restricted target is withheld from it.
    local_labels: Arc<BTreeMap<String, String>>,
}

/// Serve provider signals, following the multi-target exporter pattern.
///
/// `target` names one provider, and `collect[]` names the signals wanted.
/// Both absent returns everything held, which is what the local gateway asks
/// for. A peer names the targets it wants, so what crosses a boundary is what
/// the reader asked for rather than whatever this site happens to hold.
///
/// Samples carry no timestamp. A target this operator has stopped refreshing
/// stops appearing, so a scraper's own staleness handling applies.
async fn signals_handler(
    axum::extract::State(published): axum::extract::State<Published>,
    axum::Extension(caller): axum::Extension<Caller>,
    axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>,
) -> axum::response::Response {
    let target = params.iter().find(|(k, _)| k == "target").map(|(_, v)| v.as_str());
    let collect: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == "collect[]" || k == "collect")
        .map(|(_, v)| v.clone())
        .collect();

    // Scope comes from the connection, so no parameter can widen it.
    // Holding nothing for a peer serves it nothing, not the no-policy default.
    let reader = match &caller {
        Caller::Local => &*published.local_labels,
        Caller::Peer(Some(labels)) => labels,
        Caller::Peer(None) => return refused(),
    };
    let reader = Some(reader);
    let (mut body, mut oldest) = published.site.render(target, &collect, reader);
    if caller == Caller::Local {
        let (relayed, relayed_age) = published.peers.render(target, &collect, reader);
        body.push_str(&relayed);
        oldest = oldest.max(relayed_age);
    }

    served(body, oldest)
}

/// Refuse a caller this site will not serve.
///
/// A status rather than an empty body, because the two mean different things to
/// whoever is reading. An empty exposition says nothing is held right now, which
/// a peer operator would chase as a fault; a refusal says the answer will not
/// change until an administrator changes it.
///
/// The reason is deliberately the same for every cause. A caller learns that it
/// is refused, and not whether the name is unknown, the record refuses reads, or
/// a pinned key did not match.
fn refused() -> axum::response::Response {
    (
        http::StatusCode::FORBIDDEN,
        "signals: caller is not permitted to read this site\n",
    )
        .into_response()
}

/// One exposition response, with `Age` bounding the whole body.
fn served(body: String, oldest: std::time::Duration) -> axum::response::Response {
    (
        [
            (http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
            (http::header::AGE, &*oldest.as_secs().to_string()),
        ],
        body,
    )
        .into_response()
}

/// Prometheus text-format metrics handler.
async fn metrics_handler() -> impl axum::response::IntoResponse {
    let body = operator::metrics::encode_metrics();
    (
        [(http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Health check handler for liveness and readiness probes.
async fn health_handler() -> &'static str {
    "ok"
}

/// Path to the grid CA certificate, used to check a peer's certificate.
const GRID_CA_CERT_PATH: &str = "GRID_CA_CERT_PATH";
/// Path to this site's certificate, broadcast so peers can check its signature.
const GRID_SITE_CERT_PATH: &str = "GRID_SITE_CERT_PATH";
/// Path to this site's private key, used to sign outbound broadcasts.
const GRID_SITE_KEY_PATH: &str = "GRID_SITE_KEY_PATH";

/// Read the identity this site gossips under, from the mounted grid TLS secret.
///
/// All three have to be present. Returning `None` when any is missing keeps the
/// runtime honest about which mode it is in, rather than half-configuring a
/// node that signs but cannot verify.
fn load_grid_identity() -> Option<swim_runtime::GridIdentity> {
    let ca_path = std::env::var(GRID_CA_CERT_PATH).ok()?;
    let cert_path = std::env::var(GRID_SITE_CERT_PATH).ok()?;
    let key_path = std::env::var(GRID_SITE_KEY_PATH).ok()?;

    let read = |path: &str, what: &str| match std::fs::read_to_string(path) {
        Ok(contents) => Some(contents),
        Err(error) => {
            tracing::error!(%error, path, "could not read {what}; gossip identity not configured");
            None
        },
    };

    let grid_ca_pem = read(&ca_path, "the grid CA certificate")?;
    let site_cert_pem = read(&cert_path, "this site's certificate")?;
    let site_key_pem = read(&key_path, "this site's private key")?;

    // The signer takes PKCS#8 DER; the mounted file is PEM.
    let site_key_der = match pem::parse(&site_key_pem) {
        Ok(parsed) => parsed.contents().to_vec(),
        Err(error) => {
            tracing::error!(%error, "site private key is not valid PEM; gossip identity not configured");
            return None;
        },
    };

    tracing::info!("gossip identity configured: broadcasts are signed and peers are verified");
    Some(swim_runtime::GridIdentity {
        grid_ca_pem,
        site_cert_pem,
        site_key_der,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store holding one observation attributed to `site`.
    fn store_for(site: &str) -> operator::signals::SignalStore {
        let store = operator::signals::SignalStore::new();
        let observations = operator::signals::attribute(
            operator::signals::parse("llm_d_epp_average_queue_size{name=\"pool\"} 7"),
            site,
            "provider",
        );
        let mut collected = BTreeMap::new();
        collected.insert("provider".to_owned(), observations);
        store.refresh(collected);
        store
    }

    async fn body_for(caller: Caller) -> String {
        body_for_scoped(caller, store_for("this-site"), Arc::new(BTreeMap::new())).await
    }

    /// Serve one request against a given local store and reader identity.
    async fn body_for_scoped(
        caller: Caller,
        site: operator::signals::SignalStore,
        local_labels: Arc<BTreeMap<String, String>>,
    ) -> String {
        let (_, body) = status_and_body(caller, site, local_labels).await;
        body
    }

    /// Serve one request and report both what it answered and what it said.
    async fn status_and_body(
        caller: Caller,
        site: operator::signals::SignalStore,
        local_labels: Arc<BTreeMap<String, String>>,
    ) -> (http::StatusCode, String) {
        let published = Published {
            site,
            peers: store_for("a-peer"),
            local_labels,
        };
        let response = signals_handler(
            axum::extract::State(published),
            axum::Extension(caller),
            axum::extract::Query(Vec::new()),
        )
        .await;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap_or_default();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    /// A caller presenting `leaf`, against a site whose own key is `own`.
    fn caller_of(leaf: Option<&[u8]>, own: Option<&str>) -> Caller {
        let der = leaf.map(|b| vec![rustls::pki_types::CertificateDer::from(b.to_vec())]);
        caller_for(
            der.as_deref(),
            &SignalsIdentity {
                peers: operator::signals::PeerIdentities::new(),
                own_key: own.map(ToOwned::to_owned),
            },
            "10.0.0.1:1".parse().unwrap_or_else(|_| std::process::abort()),
        )
    }

    #[test]
    fn presenting_nothing_is_not_a_way_to_be_local() {
        // Closes the bypass: a refused peer reconnecting bare read as local.
        assert_eq!(
            caller_of(None, Some("aa")),
            Caller::Peer(None),
            "no certificate names nobody, so a refusal cannot be undone by omitting one"
        );
    }

    #[test]
    fn this_sites_own_key_is_recognised_without_being_declared() {
        let own = operator::signals::leaf_fingerprint(b"site-cert");
        assert_eq!(
            caller_of(Some(b"site-cert"), Some(&own)),
            Caller::Local,
            "a workload holding this site's certificate speaks for this site"
        );
    }

    #[test]
    fn another_key_is_not_this_site() {
        let own = operator::signals::leaf_fingerprint(b"site-cert");
        assert_eq!(
            caller_of(Some(b"somebody-else"), Some(&own)),
            Caller::Peer(None),
            "an undeclared key is a peer this site holds nothing for, never local"
        );
    }

    #[tokio::test]
    async fn a_refused_peer_is_told_so() {
        // B must learn the answer will not change, not chase an empty body.
        let (status, body) =
            status_and_body(Caller::Peer(None), store_for("this-site"), Arc::new(BTreeMap::new())).await;
        assert_eq!(
            status,
            http::StatusCode::FORBIDDEN,
            "a refusal is a status, not silence"
        );
        assert!(!body.contains("this-site"), "and carries none of this site: {body}");
    }

    #[tokio::test]
    async fn a_named_peer_is_served_this_site() {
        let body = body_for(Caller::Peer(Some(BTreeMap::new()))).await;
        assert!(
            body.contains("this-site"),
            "a placed peer still reads this site: {body}"
        );
        assert!(!body.contains("a-peer"), "and nothing relayed about another: {body}");
    }


    #[tokio::test]
    async fn a_local_consumer_receives_everything_held() {
        let body = body_for(Caller::Local).await;
        assert!(body.contains("this-site"), "a local caller receives this site: {body}");
        assert!(body.contains("a-peer"), "and everything relayed: {body}");
    }

    #[test]
    fn lease_from_seeds_reserves_full_disjoint_block() {
        let first = lease_from_seeds(100, 7).unwrap_or_else(|_| std::process::abort());
        let second = lease_from_seeds(
            first
                .last_revision
                .checked_add(1)
                .unwrap_or_else(|| std::process::abort()),
            first
                .last_node_generation
                .checked_add(1)
                .unwrap_or_else(|| std::process::abort()),
        )
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(first.first_revision, 100);
        assert_eq!(first.last_revision - first.first_revision + 1, REVISION_LEASE_SIZE);
        assert!(second.first_revision > first.last_revision);
        assert!(second.first_node_generation > first.last_node_generation);
    }

    #[test]
    fn persisted_values_win_when_clock_is_behind() {
        let future_revision = 10_000_000_000_000_u64;
        let future_generation = 10_000_000_000_000_000_000_u64;
        let lease = next_revision_lease(future_revision, future_generation).unwrap_or_else(|_| std::process::abort());
        assert_eq!(lease.first_revision, future_revision + 1);
        assert_eq!(lease.first_node_generation, future_generation + 1);
    }

    #[test]
    fn exhausted_revision_or_generation_fails_closed() {
        assert!(
            next_revision_lease(u64::MAX, 1).is_err(),
            "u64::MAX revision must overflow"
        );
        assert!(
            next_revision_lease(1, u64::MAX).is_err(),
            "u64::MAX generation must overflow"
        );
        assert!(lease_from_seeds(u64::MAX, 1).is_err(), "u64::MAX seed must overflow");
    }

    #[test]
    fn reservation_data_round_trips() {
        let lease = RevisionLease {
            first_revision: 10,
            last_revision: 20,
            first_node_generation: 30,
            last_node_generation: 40,
        };
        let data = revision_lease_data(&lease);
        assert_eq!(parse_revision_value(&data, REVISION_HIGH_KEY), Some(20));
        assert_eq!(parse_revision_value(&data, NODE_GENERATION_HIGH_KEY), Some(40));
    }
}
