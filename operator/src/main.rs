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

    // Held for the process lifetime. Dropping it also triggers, so an unwind
    // anywhere still tells the pollers to stand down rather than leaving them
    // waiting on a signal nobody is left to send.
    let (trigger, shutdown) = operator::shutdown::Trigger::new();
    tokio::spawn(watch_for_termination(trigger));

    let result = tokio::try_join!(
        run_network_controller(client.clone(), Arc::clone(&ctx)),
        run_site_controller(client.clone()),
        run_provider_controller(client.clone()),
        run_metrics_server(),
        run_signals_server(ctx.signals(), ctx.peers()),
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
async fn run_signals_server(
    site: operator::signals::SignalStore,
    peers: operator::signals::SignalStore,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("GRID_SIGNALS_ADDR").unwrap_or_else(|_| "0.0.0.0:9091".to_owned());
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(signals_handler))
        // Until the listener terminates TLS, no caller can present a peer
        // certificate, so every request is treated as in-cluster.
        .layer(axum::Extension(Caller::Local))
        .with_state(Published { site, peers });
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound_addr = listener.local_addr().map_or_else(|_| addr.clone(), |a| a.to_string());
    tracing::info!(addr = %bound_addr, "signals server started");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Scrape this site's providers on their own interval and publish the result.
///
/// Separate from reconcile, which runs on an interval sized for declarations
/// and is two orders of magnitude slower than these values move.
async fn run_local_scraper(
    ctx: Arc<OperatorCtx>,
    client: Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Milliseconds take precedence, because the second-granularity setting
    // cannot express an interval shorter than one and a local scrape crosses no
    // network. The seconds form stays for anything already setting it.
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
    let (source, scheme) = peer_source(&client, shutdown.clone()).await;
    tracing::info!(
        port,
        interval_secs = interval.as_secs(),
        source = source.name(),
        scheme,
        "peer signals poller started"
    );

    poll_until_shutdown(&ctx, &swim, source.as_ref(), port, scheme, interval, &shutdown).await;
    tracing::info!("peer signals poller stopped");
    Ok(())
}

/// Poll on the interval until told to stand down.
///
/// The signal is checked in place of the tick rather than after it, so a poller
/// that has just slept out a thirty second interval does not start one more
/// round of cross-cluster requests on the way out.
#[expect(clippy::too_many_arguments, reason = "a poll loop needs its whole configuration")]
async fn poll_until_shutdown(
    ctx: &Arc<OperatorCtx>,
    swim: &Arc<swim_runtime::SwimHandle>,
    source: &dyn operator::signals::PeerSignals,
    port: u16,
    scheme: &str,
    interval: std::time::Duration,
    shutdown: &operator::shutdown::Shutdown,
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
    let snapshot = swim.snapshot();
    let alive = snapshot
        .members
        .iter()
        .filter(|m| m.status == operator::swim::MemberStatus::Alive)
        .map(|m| (m.site_id.as_str(), m.endpoint.as_str()));
    let sites = operator::signals::peer_sites(alive, swim.site_name(), port, scheme);
    if sites.is_empty() {
        return;
    }
    let collected = source.collect(&sites).await;
    ctx.peers().refresh(collected.into_iter().collect(), peer_ttl());
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

/// How long a peer's data stays served without a successful poll.
///
/// Must exceed the peer poll interval, so one failed poll does not remove a
/// site that is otherwise healthy.
fn peer_ttl() -> std::time::Duration {
    std::time::Duration::from_secs(parse_env_or("GRID_SIGNALS_PEER_TTL_SECS", 90_u64))
}

/// Client TLS for peer polling, or `None` when the network declares no trust.
///
/// Logged rather than fatal: a site with no TLS material still polls, and the
/// scheme in the poller's startup line says which mode it is in.
async fn peer_tls(client: &Client) -> Option<Arc<rustls::ClientConfig>> {
    // Off unless asked for. The signals listener does not terminate TLS, so a
    // client that upgrades on its own speaks TLS to a plaintext server and
    // every poll fails as a transport error. Deriving this from whether any
    // material happened to exist meant installing a certificate for something
    // else switched peer polling off, which is the opposite of what installing
    // a certificate should do.
    //
    // Remove the gate once the listener terminates TLS, at which point the
    // scope rule has a certificate to decide from as well.
    if !parse_env_or("GRID_SIGNALS_PEER_TLS", false) {
        return None;
    }
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
#[expect(
    dead_code,
    reason = "Peer is set once the listener terminates TLS; until then no caller can present a certificate"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Caller {
    /// Another site.
    Peer,
    /// A consumer inside this cluster.
    Local,
}

/// Everything this operator publishes.
#[derive(Clone)]
struct Published {
    /// This site's own signals.
    site: operator::signals::SignalStore,
    /// What peers reported about themselves.
    peers: operator::signals::SignalStore,
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

    // Scope is decided from the connection before any parameter is read, so a
    // request cannot widen it. A peer receives this site alone.
    let (mut body, mut oldest) = published.site.render(target, &collect);
    if caller == Caller::Local {
        let (relayed, relayed_age) = published.peers.render(target, &collect);
        body.push_str(&relayed);
        oldest = oldest.max(relayed_age);
    }

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

#[cfg(test)]
mod tests {
    use super::*;

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
