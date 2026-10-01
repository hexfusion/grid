//! [`GridSite`] controller.
//!
//! Reconciles [`GridSite`] resources: validates the grid network
//! reference, manages lifecycle phase transitions, and maintains
//! the trust bundle secret.
//!
//! [`GridSite`]: crate::crd::grid_site::GridSite

use std::sync::Arc;

use k8s_openapi::api::core::v1::ObjectReference;
use kube::{
    Client, Resource as _,
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
    },
};
use tokio::time::Duration;
use tracing::info;
use zeroize::Zeroizing;

use crate::{
    crd::{
        condition::{self, Rejection},
        grid_network::{GridNetwork, PeerTrustMode, TlsMode},
        grid_site::{GridSite, GridSitePhase, GridSiteStatus},
    },
    error::OperatorError,
    resources::{
        gateway_probe::{
            CanonicalFingerprint, GatewayProbeOutcome, probe_transition, validate_canonical_pins, validate_server_name,
        },
        secret::read_secret_bytes,
        tls_probe::{
            PeerIdentity, build_tls_config, parse_ca_roots, parse_client_certs, parse_private_key, probe_gateway,
        },
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Requeue interval after a successful reconciliation.
///
/// Kept at 60 s so that Secret or trust-policy rotation is observed
/// within one minute without requiring a dedicated Secret watch.
const REQUEUE_INTERVAL: Duration = Duration::from_secs(60);

/// TCP connect timeout for plaintext gateway reachability probes.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Least and most time between probes of an `Unreachable` site, which backs off as it stays down.
const UNREACHABLE_BACKOFF: (Duration, Duration) = (Duration::from_secs(30), Duration::from_secs(300));

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

/// The event reference for a cluster-scoped site, placed in `namespace`.
fn event_reference(site: &GridSite, namespace: &str) -> ObjectReference {
    ObjectReference {
        namespace: Some(namespace.to_owned()),
        ..site.object_ref(&())
    }
}

/// Reconcile a [`GridSite`] resource.
///
/// # Errors
///
/// Returns [`OperatorError`] on Kubernetes API failures.
#[expect(
    clippy::large_stack_frames,
    clippy::too_many_lines,
    reason = "TLS material loading + event recording requires intermediaries; \
              splitting hides the reconciliation flow"
)]
pub async fn reconcile(site: Arc<GridSite>, client: Arc<Client>) -> Result<Action, OperatorError> {
    let name = site.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    let reporter = Reporter {
        controller: "grid-site-controller".into(),
        instance: None,
    };
    let object_ref = event_reference(&site, client.default_namespace());
    let recorder = Recorder::new(client.as_ref().clone(), reporter);

    info!(name, "reconciling GridSite");

    let network = fetch_network(&site, client.as_ref()).await?;
    let current_phase = site.status.as_ref().map_or(&GridSitePhase::Pending, |s| &s.phase);

    let rejection = site_spec_rejection(&site, &network);
    let outcome = if rejection.is_none() && needs_probe(current_phase) {
        let start = std::time::Instant::now();
        let result = evaluate_gateway(&site, client.as_ref(), &network).await;
        let tls_mode = if egress_tls_mode(&site, &network) == TlsMode::Plaintext {
            "Plaintext"
        } else {
            "Mutual"
        };
        crate::metrics::record_probe(result.as_reason(), tls_mode, start.elapsed());
        Some(result)
    } else {
        None
    };

    let probed = outcome.is_some();
    let (next_phase, reason, message) = if let Some(rejection) = rejection {
        (
            rejected_phase(current_phase),
            rejection.reason.to_owned(),
            rejection.message,
        )
    } else {
        let (phase, reason, message) = site_phase_next(current_phase, &site, outcome.as_ref());
        let message = identity_message(&reason, &site, &network).unwrap_or(message);
        (phase, reason, message)
    };
    Box::pin(update_status(
        &site,
        client.as_ref(),
        &next_phase,
        &reason,
        &message,
        probed,
        &recorder,
        &object_ref,
    ))
    .await?;

    let now = time::OffsetDateTime::now_utc();
    Ok(Action::requeue(requeue_after(&next_phase, site.status.as_ref(), now)))
}

/// Next probe delay: the usual interval, or for an `Unreachable` site half its time down, bounded, plus jitter.
fn requeue_after(next: &GridSitePhase, previous: Option<&GridSiteStatus>, now: time::OffsetDateTime) -> Duration {
    if *next != GridSitePhase::Unreachable {
        return REQUEUE_INTERVAL;
    }
    let down_since = previous
        .filter(|status| status.phase == GridSitePhase::Unreachable)
        .and_then(|status| condition::find(&status.conditions, condition::CONNECTED))
        .and_then(|connected| {
            time::OffsetDateTime::parse(
                &connected.last_transition_time,
                &time::format_description::well_known::Rfc3339,
            )
            .ok()
        });
    let down = down_since.map_or(Duration::ZERO, |since| {
        Duration::try_from(now - since).unwrap_or_default()
    });
    let (least, most) = UNREACHABLE_BACKOFF;
    let base = (down / 2).clamp(least, most);
    // Spread retries of sites that went down together.
    let jitter = base.mul_f64(f64::from(now.nanosecond() % 1_000) / 5_000.0);
    base + jitter
}

/// Error policy for the [`GridSite`] controller.
pub fn error_policy(_site: Arc<GridSite>, error: &OperatorError, _ctx: Arc<Client>) -> Action {
    tracing::error!(%error, "GridSite reconciliation failed");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// Network lookup
// ---------------------------------------------------------------------------

/// Fetch the referenced [`GridNetwork`].
async fn fetch_network(site: &GridSite, client: &Client) -> Result<GridNetwork, OperatorError> {
    let api: Api<GridNetwork> = Api::all(client.clone());
    let network_name = &site.spec.grid_network_ref;
    api.get(network_name).await.map_err(|e| {
        tracing::warn!(error = %e, network = %network_name, "lookup failed");
        OperatorError::NotFound(format!("GridNetwork {network_name}"))
    })
}

/// Whether the current phase requires a gateway probe.
fn needs_probe(phase: &GridSitePhase) -> bool {
    matches!(
        phase,
        GridSitePhase::Connecting | GridSitePhase::Active | GridSitePhase::Unreachable
    )
}

// ---------------------------------------------------------------------------
// Phase Determination
// ---------------------------------------------------------------------------

/// Determine the next lifecycle phase for a [`GridSite`].
///
/// Pure function: the caller supplies the probe `outcome` (from
/// [`evaluate_gateway`]) for phases that require it.  Phases that do
/// not need a probe (`Pending`, `Discovered`, `Left`) ignore `outcome`.
///
/// Returns `(next_phase, reason, message)`.  `reason` is machine-readable;
/// `message` is human-readable and never contains private material.
#[expect(
    clippy::too_many_lines,
    reason = "match arms are individually trivial; splitting would fragment the state machine"
)]
pub(crate) fn site_phase_next(
    current: &GridSitePhase,
    site: &GridSite,
    outcome: Option<&GatewayProbeOutcome>,
) -> (GridSitePhase, String, String) {
    let has_egress_address = egress_address(site).is_some();

    match current {
        GridSitePhase::Pending => (
            GridSitePhase::Pending,
            "AwaitingDiscovery".to_owned(),
            "site record created; waiting for SWIM discovery to advance to Discovered".to_owned(),
        ),
        GridSitePhase::Discovered => {
            if gossip_address_refused(site) {
                (
                    GridSitePhase::Discovered,
                    "GossipedAddressRefused".to_owned(),
                    "gossiped gateway address is not a dialable literal IP:port; set spec.egress.address".to_owned(),
                )
            } else if has_egress_address {
                (
                    GridSitePhase::Connecting,
                    "GatewayAddressKnown".to_owned(),
                    "gateway address present; awaiting control-plane trust verification".to_owned(),
                )
            } else {
                (
                    GridSitePhase::Discovered,
                    "GatewayAddressMissing".to_owned(),
                    "gateway address not yet available; cannot advance to Connecting".to_owned(),
                )
            }
        },
        GridSitePhase::Left => (
            GridSitePhase::Left,
            "Left".to_owned(),
            "site has left the grid".to_owned(),
        ),
        GridSitePhase::Connecting | GridSitePhase::Active | GridSitePhase::Unreachable => {
            let outcome = outcome.unwrap_or(&GatewayProbeOutcome::AddressMissing);
            let t = probe_transition(current, outcome);
            (t.phase, t.reason.to_owned(), t.message)
        },
    }
}

// ---------------------------------------------------------------------------
// Gateway evaluation
// ---------------------------------------------------------------------------

/// Evaluate the gateway health of a [`GridSite`].
///
/// For plaintext transport, performs a bounded TCP probe for diagnostics.
/// Plaintext reachability never makes a site routing-eligible.
/// For TLS transport, loads trust material from Kubernetes Secrets
/// and performs a bounded TLS handshake with certificate verification.
///
/// Never leaks private key material in the returned outcome.
async fn evaluate_gateway(site: &GridSite, client: &Client, network: &GridNetwork) -> GatewayProbeOutcome {
    let Some(addr) = egress_address(site) else {
        return GatewayProbeOutcome::AddressMissing;
    };

    if egress_tls_mode(site, network) == TlsMode::Plaintext {
        return if tcp_probe(addr).await {
            GatewayProbeOutcome::PlaintextReachable
        } else {
            GatewayProbeOutcome::PlaintextUnreachable
        };
    }

    match build_probe_config_from_secrets(site, addr, client, network).await {
        Ok(config) => probe_gateway(&config).await,
        Err(outcome) => outcome,
    }
}

/// Build a `ProbeConfig` by loading trust material from Kubernetes
/// Secrets referenced by the `GridNetwork`.
///
/// Returns a `GatewayProbeOutcome` on failure so the caller can
/// report the precise failure mode.
#[expect(
    clippy::too_many_lines,
    reason = "linear secret-loading sequence; splitting would fragment error provenance"
)]
#[expect(
    clippy::large_stack_frames,
    reason = "TLS material parsing requires several Vec<u8> intermediaries"
)]
async fn build_probe_config_from_secrets(
    site: &GridSite,
    addr: &str,
    client: &Client,
    network: &GridNetwork,
) -> Result<crate::resources::tls_probe::ProbeConfig, GatewayProbeOutcome> {
    use GatewayProbeOutcome as O;

    let ca_ref = network.spec.tls.ca_secret_ref.as_ref().ok_or(O::TrustMaterialMissing)?;
    let ca_bytes = read_secret_bytes(client, ca_ref, "ca.crt")
        .await
        .map_err(|_err| O::TrustMaterialMissing)?
        .into_bytes()
        .ok_or(O::TrustMaterialMissing)?;
    let roots = parse_ca_roots(&ca_bytes).map_err(|_err| O::TrustMaterialInvalid)?;

    let secret_ref = network
        .spec
        .tls
        .site_secret_ref
        .as_ref()
        .ok_or(O::TrustMaterialMissing)?;
    let cert_bytes = read_secret_bytes(client, secret_ref, "tls.crt")
        .await
        .map_err(|_err| O::TrustMaterialMissing)?
        .into_bytes()
        .ok_or(O::TrustMaterialMissing)?;
    let key_bytes = Zeroizing::new(
        read_secret_bytes(client, secret_ref, "tls.key")
            .await
            .map_err(|_err| O::TrustMaterialMissing)?
            .into_bytes()
            .ok_or(O::TrustMaterialMissing)?,
    );
    let client_certs = parse_client_certs(&cert_bytes).map_err(|_err| O::TrustMaterialInvalid)?;
    let client_key = parse_private_key(&key_bytes).map_err(|_err| O::TrustMaterialInvalid)?;

    let tls_config =
        build_tls_config(roots, Some(client_certs), Some(client_key)).map_err(|_err| O::TrustMaterialInvalid)?;

    let server_name_str = probe_server_name(site);
    validate_server_name(&server_name_str).map_err(|_err| O::TrustMaterialInvalid)?;
    let server_name =
        crate::resources::tls_backend::parse_server_name(&server_name_str).map_err(|_err| O::TrustMaterialInvalid)?;

    let identity = if spiffe_peer_trust(network) {
        PeerIdentity::Spiffe(certs::spiffe_id(site.metadata.name.as_deref().unwrap_or_default()))
    } else {
        PeerIdentity::Pins(resolve_pins(site)?)
    };

    Ok(crate::resources::tls_probe::ProbeConfig {
        address: addr.to_owned(),
        tls_config,
        server_name,
        identity,
    })
}

/// The declared `serverName`, else `<site>.grid.internal`, the DNS SAN enrollment issues.
fn probe_server_name(site: &GridSite) -> String {
    site.spec
        .egress
        .as_ref()
        .and_then(|egress| egress.tls.server_name.as_deref())
        .filter(|name| !name.trim().is_empty())
        .map_or_else(
            || format!("{}.grid.internal", site.metadata.name.as_deref().unwrap_or_default()),
            ToOwned::to_owned,
        )
}

/// The status message naming what the handshake checked, for the reasons that depend on the peer trust mode.
fn identity_message(reason: &str, site: &GridSite, network: &GridNetwork) -> Option<String> {
    let spiffe = spiffe_peer_trust(network);
    let expected = if spiffe {
        format!(
            "SPIFFE ID {}",
            certs::spiffe_id(site.metadata.name.as_deref().unwrap_or_default())
        )
    } else {
        let pins: Vec<&str> = site
            .spec
            .trust
            .iter()
            .flat_map(|trust| trust.canonical_fingerprints.iter().flatten())
            .map(|pin| pin.get(..12).unwrap_or(pin))
            .collect();
        format!("pinned leaf digest {}", pins.join(" or "))
    };
    match reason {
        "TlsVerified" => Some(format!("TLS handshake verified: chain to the Grid CA and {expected}")),
        "PinMismatch" => Some(format!("server leaf does not match the {expected}")),
        "IdentityMismatch" if spiffe => Some(format!(
            "server cert does not match serverName {} or carry {expected}",
            probe_server_name(site)
        )),
        _ => None,
    }
}

/// Whether the network verifies peers by SPIFFE ID rather than pins.
fn spiffe_peer_trust(network: &GridNetwork) -> bool {
    network
        .spec
        .peer_trust
        .as_ref()
        .is_some_and(|trust| trust.mode == PeerTrustMode::Spiffe)
}

/// Resolve the canonical fingerprint pins from the [`GridSite`] trust policy.
///
/// Missing pin policy is reported separately from malformed pin policy so
/// operators can distinguish incomplete bootstrap from invalid configuration.
fn resolve_pins(site: &GridSite) -> Result<Vec<CanonicalFingerprint>, GatewayProbeOutcome> {
    use GatewayProbeOutcome as O;

    let Some(trust) = site.spec.trust.as_ref() else {
        return Err(O::TrustMaterialMissing);
    };

    match trust.canonical_fingerprints.as_ref() {
        Some(fps) => validate_canonical_pins(fps).map_err(|e| {
            tracing::warn!(error = %e, "canonical pin validation failed");
            O::TrustMaterialInvalid
        }),
        None => Err(O::TrustMaterialMissing),
    }
}

/// Bounded label for a [`GridSitePhase`] value in metrics.
fn phase_label(phase: &GridSitePhase) -> &'static str {
    match phase {
        GridSitePhase::Pending => "Pending",
        GridSitePhase::Discovered => "Discovered",
        GridSitePhase::Connecting => "Connecting",
        GridSitePhase::Active => "Active",
        GridSitePhase::Unreachable => "Unreachable",
        GridSitePhase::Left => "Left",
    }
}

/// The first spec rule `site` breaks against its `network`; a rejected site is never probed.
pub(crate) fn site_spec_rejection(site: &GridSite, network: &GridNetwork) -> Option<Rejection> {
    let spiffe = spiffe_peer_trust(network);
    let pinned = site
        .spec
        .trust
        .as_ref()
        .and_then(|trust| trust.canonical_fingerprints.as_ref())
        .is_some_and(|pins| !pins.is_empty());
    if spiffe && pinned {
        return Some(Rejection::new(
            "TrustConflictsWithPeerTrust",
            "spec.trust pins are ignored while the GridNetwork peerTrust.mode is spiffe; remove them",
        ));
    }
    site.spec.egress.as_ref().and_then(egress_rejection)
}

/// The SNI rule a declared `spec.egress` breaks, if any.
fn egress_rejection(egress: &crate::crd::grid_site::EgressConfig) -> Option<Rejection> {
    let server_name = egress.tls.server_name.as_deref().filter(|name| !name.trim().is_empty());
    match (egress.tls.mode, server_name) {
        (TlsMode::Plaintext, Some(_)) => Some(Rejection::new(
            "ServerNameForbidden",
            "spec.egress.tls.mode plaintext refuses spec.egress.tls.serverName",
        )),
        (TlsMode::MutualTls, Some(name)) if validate_server_name(name).is_err() => Some(Rejection::new(
            "ServerNameInvalid",
            "spec.egress.tls.serverName is not a valid DNS name",
        )),
        (TlsMode::MutualTls, Some(_) | None) | (TlsMode::Plaintext, None) => None,
    }
}

/// The phase a rejected site holds: never `Active`, so it never routes.
fn rejected_phase(current: &GridSitePhase) -> GridSitePhase {
    match current {
        GridSitePhase::Active | GridSitePhase::Unreachable => GridSitePhase::Connecting,
        GridSitePhase::Pending | GridSitePhase::Discovered | GridSitePhase::Connecting | GridSitePhase::Left => {
            current.clone()
        },
    }
}

/// Probe target: the declared `spec.egress` address, else a dialable gossiped one.
pub(crate) fn egress_address(site: &GridSite) -> Option<&str> {
    declared_egress_address(site).or_else(|| gossiped_egress_address(site).filter(|addr| is_dialable_gossip(addr)))
}

/// The non-blank `spec.egress.address`, which may name a host.
fn declared_egress_address(site: &GridSite) -> Option<&str> {
    site.spec
        .egress
        .as_ref()
        .map(|egress| egress.address.as_str())
        .filter(|addr| !addr.trim().is_empty())
}

/// The non-blank `status.discovered.egressAddress`, as gossiped.
fn gossiped_egress_address(site: &GridSite) -> Option<&str> {
    site.status
        .as_ref()
        .and_then(|status| status.discovered.as_ref())
        .and_then(|discovered| discovered.egress_address.as_deref())
        .filter(|addr| !addr.trim().is_empty())
}

/// Whether a gossiped address is a literal `IP:port` off loopback, link-local, unspecified, and metadata.
fn is_dialable_gossip(addr: &str) -> bool {
    addr.parse::<std::net::SocketAddr>()
        .is_ok_and(|socket| socket.port() != 0 && crate::signals::is_dialable_ip(socket.ip()))
}

/// Whether the only address on offer is a gossiped one the dial guard refuses.
fn gossip_address_refused(site: &GridSite) -> bool {
    declared_egress_address(site).is_none()
        && gossiped_egress_address(site).is_some_and(|addr| !is_dialable_gossip(addr))
}

/// Egress TLS mode: the declared one, else the network's `siteDiscovery.defaultEgressTls`.
fn egress_tls_mode(site: &GridSite, network: &GridNetwork) -> TlsMode {
    site.spec
        .egress
        .as_ref()
        .map_or(network.spec.site_discovery.default_egress_tls, |egress| egress.tls.mode)
}

/// Attempt a TCP connection to `addr` with [`PROBE_TIMEOUT`].
///
/// Returns `true` if the connection succeeds within the timeout, `false`
/// otherwise.  Used only for plaintext transport probes.
async fn tcp_probe(addr: &str) -> bool {
    tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(addr))
        .await
        .is_ok_and(|r| r.is_ok())
}

// ---------------------------------------------------------------------------
// Status Update
// ---------------------------------------------------------------------------

/// Patch the `GridSite` status subresource.
///
/// Patches only fields owned by this controller. `capabilities` and
/// `discovered` are owned by SWIM reconciliation and are deliberately
/// omitted. Updates `last_probe_time` when a probe was executed; each condition
/// keeps its own transition time.
#[expect(
    clippy::too_many_lines,
    reason = "linear status-patching sequence; splitting would fragment field ownership"
)]
#[expect(
    clippy::too_many_arguments,
    clippy::large_stack_frames,
    clippy::cognitive_complexity,
    reason = "status patch requires phase, reason, message, and probe flag from the reconcile caller; \
              CAS conflict handling + mutation detection adds branches"
)]
async fn update_status(
    site: &GridSite,
    client: &Client,
    phase: &GridSitePhase,
    reason: &str,
    message: &str,
    probed: bool,
    recorder: &Recorder,
    object_ref: &ObjectReference,
) -> Result<(), OperatorError> {
    let name = site.metadata.name.as_deref().unwrap_or_else(|| std::process::abort());

    let existing = site.status.as_ref();
    let current_phase = existing.map(|s| &s.phase);
    let phase_changed = current_phase != Some(phase);

    let now = rfc3339_now();
    let probe_time = if probed {
        now.clone()
    } else {
        existing.and_then(|s| s.last_probe_time.clone())
    };

    let observed_generation = site.metadata.generation.unwrap_or(0);
    let conditions = condition::refresh(
        existing.map(|s| s.conditions.as_slice()),
        condition::site_conditions(phase, reason, message),
        observed_generation,
    );
    let api: Api<GridSite> = Api::all(client.clone());
    let status = GridSiteStatus {
        conditions,
        phase: phase.clone(),
        observed_generation,
        capabilities: existing.map_or_else(Default::default, |s| s.capabilities.clone()),
        last_probe_time: probe_time,
        discovered: existing.and_then(|s| s.discovered.clone()),
    };

    if !grid_site_status_needs_update(existing, &status) {
        return Ok(());
    }

    // Patch only fields owned by this controller. The GridNetwork controller
    // updates capabilities and discovered independently; replacing the
    // complete status object here could overwrite a newer SWIM observation.
    //
    // Include metadata.resourceVersion as a CAS precondition so the API
    // server returns 409 Conflict if another replica already wrote a newer
    // version. On conflict we yield silently — the informer will deliver
    // the updated object on the next reconcile.
    let rv = site.metadata.resource_version.as_deref();
    let patch = grid_site_owned_status_patch(&status, rv);

    let patched = match api
        .patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
        .await
    {
        Ok(p) => p,
        Err(kube::Error::Api(e)) if e.code == 409 => {
            tracing::debug!(
                grid_site = name,
                "status patch conflict — another replica won the CAS race"
            );
            return Ok(());
        },
        Err(e) => return Err(e.into()),
    };

    // Mutation detection: compare resourceVersion before/after to gate
    // Event emission and metric recording.
    let patched_rv = patched.metadata.resource_version.as_deref();
    let patch_caused_mutation = rv != patched_rv;

    let reason_changed = existing.is_none_or(|current| last_reason(current) != Some(reason));
    if patch_caused_mutation && (phase_changed || reason_changed) {
        tracing::info!(
            grid_site = name,
            previous_phase = ?current_phase,
            phase = ?phase,
            reason,
            "GridSite gateway health state changed"
        );

        let event_type = event_type_for_reason(reason);
        let event_note = truncate_event_note(message);
        if let Err(e) = recorder
            .publish(
                &Event {
                    type_: event_type,
                    reason: reason.to_owned(),
                    note: Some(event_note),
                    action: "GatewayProbe".to_owned(),
                    secondary: None,
                },
                object_ref,
            )
            .await
        {
            tracing::warn!(error = %e, "failed to publish GridSite event");
        }

        let from_label = current_phase.map_or("None", phase_label);
        crate::metrics::record_phase_transition(from_label, phase_label(phase), reason);
    }

    Ok(())
}

/// Map a status reason to a Kubernetes [`EventType`].
///
/// [`Normal`] for successful lifecycle progressions and expected states;
/// [`Warning`] for trust, identity, and connectivity failures.
///
/// [`Normal`]: EventType::Normal
/// [`Warning`]: EventType::Warning
fn event_type_for_reason(reason: &str) -> EventType {
    match reason {
        "TlsVerified" | "AwaitingDiscovery" | "GatewayAddressKnown" | "Left" => EventType::Normal,
        _ => EventType::Warning,
    }
}

/// Truncate an event note to a bounded length.
///
/// Reuses [`MAX_STATUS_MESSAGE_LEN`](crate::resources::gateway_probe::MAX_STATUS_MESSAGE_LEN)
/// from [`gateway_probe`](crate::resources::gateway_probe) to keep
/// Event notes well within the Kubernetes soft 1 KB limit and prevent
/// accidental PEM or key leakage.
fn truncate_event_note(message: &str) -> String {
    use crate::resources::gateway_probe::MAX_STATUS_MESSAGE_LEN;
    if message.chars().count() <= MAX_STATUS_MESSAGE_LEN {
        message.to_owned()
    } else {
        let mut s: String = message.chars().take(MAX_STATUS_MESSAGE_LEN - 3).collect();
        s.push_str("...");
        s
    }
}

/// The reason last reported, read from the `Connected` condition.
fn last_reason(status: &GridSiteStatus) -> Option<&str> {
    condition::find(&status.conditions, condition::CONNECTED).map(|c| c.reason.as_str())
}

/// Each condition's type, status, reason, and generation, without the free-text message.
///
/// A probe error's wording can alternate (timeout, then refused) under one reason; writing on that alone
/// would retrigger a reconcile and skip the unreachable backoff.
fn condition_verdicts(conditions: &[condition::Condition]) -> Vec<(&str, condition::ConditionStatus, &str, i64)> {
    conditions
        .iter()
        .map(|c| (c.type_.as_str(), c.status, c.reason.as_str(), c.observed_generation))
        .collect()
}

/// Whether the controller-owned status fields differ.
///
/// `last_probe_time` is rewritten every probed reconcile, so comparing it
/// would re-patch each pass into a hot loop.
/// `capabilities` and `discovered` are SWIM-owned and not patched here.
fn grid_site_status_needs_update(current: Option<&GridSiteStatus>, desired: &GridSiteStatus) -> bool {
    let Some(current) = current else {
        return true;
    };
    current.phase != desired.phase
        || condition_verdicts(&current.conditions) != condition_verdicts(&desired.conditions)
        || current.observed_generation != desired.observed_generation
}

/// Build a merge patch containing only fields owned by the `GridSite`
/// controller, with `metadata.resourceVersion` as a CAS precondition.
fn grid_site_owned_status_patch(status: &GridSiteStatus, resource_version: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "resourceVersion": resource_version
        },
        "status": {
            "conditions": status.conditions,
            "phase": status.phase,
            "observedGeneration": status.observed_generation,
            "lastProbeTime": status.last_probe_time
        }
    })
}

/// Current UTC time as an RFC 3339 string.
///
/// Returns `None` on format failure rather than panicking.
fn rfc3339_now() -> Option<String> {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_site_status_update_is_skipped_when_semantically_unchanged() {
        let baseline = GridSiteStatus {
            phase: GridSitePhase::Active,
            observed_generation: 2,
            ..GridSiteStatus::default()
        };
        assert!(!grid_site_status_needs_update(Some(&baseline), &baseline));

        let changed = GridSiteStatus {
            phase: GridSitePhase::Unreachable,
            ..baseline.clone()
        };
        assert!(grid_site_status_needs_update(Some(&baseline), &changed));
        assert!(grid_site_status_needs_update(None, &baseline));
    }

    #[test]
    fn grid_site_status_update_is_skipped_when_only_the_probe_timestamp_changed() {
        let baseline = GridSiteStatus {
            phase: GridSitePhase::Active,
            observed_generation: 2,
            last_probe_time: Some("2026-01-01T00:00:00Z".to_owned()),
            ..GridSiteStatus::default()
        };
        // A probe that changed nothing still rewrites the timestamps, which the guard must ignore.
        let probed_again = GridSiteStatus {
            last_probe_time: Some("2026-01-01T00:00:30Z".to_owned()),
            ..baseline.clone()
        };
        assert!(
            !grid_site_status_needs_update(Some(&baseline), &probed_again),
            "a timestamp-only change must not trigger a status write"
        );
    }

    #[test]
    fn status_patch_does_not_claim_swim_owned_fields() {
        let status = GridSiteStatus {
            capabilities: crate::crd::grid_site::SiteCapabilities {
                inference: true,
                ..Default::default()
            },
            discovered: Some(crate::crd::grid_site::DiscoveredStatus {
                egress_address: Some("10.0.0.9:8443".to_owned()),
                ..Default::default()
            }),
            phase: GridSitePhase::Active,
            ..Default::default()
        };
        let patch = grid_site_owned_status_patch(&status, Some("12345"));
        let owned = patch
            .get("status")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(!owned.contains_key("capabilities"));
        assert!(!owned.contains_key("discovered"));
    }

    #[test]
    fn status_patch_carries_the_site_conditions() {
        let conditions = condition::reconcile_conditions(
            &[],
            condition::site_conditions(&GridSitePhase::Active, "Verified", ""),
            4,
            "2026-10-01T00:00:00Z",
        );
        let status = GridSiteStatus {
            conditions,
            phase: GridSitePhase::Active,
            ..Default::default()
        };
        let patch = grid_site_owned_status_patch(&status, None);
        let types: Vec<&str> = patch
            .pointer("/status/conditions")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| std::process::abort())
            .iter()
            .filter_map(|c| c.get("type").and_then(serde_json::Value::as_str))
            .collect();
        assert_eq!(types, ["Accepted", "Discovered", "Connected", "Ready"]);
    }

    #[test]
    fn a_condition_change_alone_triggers_a_status_write() {
        let current = GridSiteStatus::default();
        let desired = GridSiteStatus {
            conditions: condition::reconcile_conditions(
                &[],
                condition::site_conditions(&GridSitePhase::Pending, "", ""),
                0,
                "t",
            ),
            ..Default::default()
        };
        assert!(grid_site_status_needs_update(Some(&current), &desired));
        assert!(!grid_site_status_needs_update(Some(&desired), &desired));
    }

    #[test]
    fn a_new_probe_error_text_under_the_same_reason_writes_nothing() {
        let status = |message: &str| GridSiteStatus {
            phase: GridSitePhase::Unreachable,
            conditions: condition::reconcile_conditions(
                &[],
                condition::site_conditions(&GridSitePhase::Unreachable, "ConnectionFailed", message),
                1,
                "t",
            ),
            ..Default::default()
        };
        assert!(!grid_site_status_needs_update(
            Some(&status("connect timed out")),
            &status("connection refused")
        ));
        let other_reason = GridSiteStatus {
            conditions: condition::reconcile_conditions(
                &[],
                condition::site_conditions(&GridSitePhase::Unreachable, "ConnectTimeout", ""),
                1,
                "t",
            ),
            ..status("")
        };
        assert!(grid_site_status_needs_update(Some(&status("")), &other_reason));
    }

    #[test]
    fn the_last_reason_is_read_back_from_the_connected_condition() {
        let status = GridSiteStatus {
            conditions: condition::reconcile_conditions(
                &[],
                condition::site_conditions(&GridSitePhase::Unreachable, "PinMismatch", "pin did not match"),
                1,
                "t",
            ),
            ..Default::default()
        };
        assert_eq!(last_reason(&status), Some("PinMismatch"));
        let connected =
            condition::find(&status.conditions, condition::CONNECTED).unwrap_or_else(|| std::process::abort());
        assert_eq!(connected.message, "pin did not match");
        let patch = grid_site_owned_status_patch(&status, None);
        for flat in ["reason", "message", "lastTransitionTime"] {
            assert!(
                patch.pointer(&format!("/status/{flat}")).is_none(),
                "{flat} lives in conditions now"
            );
        }
    }

    #[test]
    fn status_patch_includes_resource_version_as_cas_precondition() {
        let status = GridSiteStatus {
            phase: GridSitePhase::Connecting,
            ..Default::default()
        };
        let patch = grid_site_owned_status_patch(&status, Some("99887"));
        let rv = patch
            .get("metadata")
            .and_then(|m| m.get("resourceVersion"))
            .and_then(serde_json::Value::as_str);
        assert_eq!(rv, Some("99887"), "patch must carry resourceVersion for CAS");
    }

    #[test]
    fn status_patch_carries_null_resource_version_when_absent() {
        let status = GridSiteStatus::default();
        let patch = grid_site_owned_status_patch(&status, None);
        let rv = patch.get("metadata").and_then(|m| m.get("resourceVersion"));
        assert!(
            rv.is_some_and(serde_json::Value::is_null),
            "patch should carry null resourceVersion when not set"
        );
    }
    use crate::crd::grid_site::{EgressConfig, EgressTls, GridSiteSpec};

    fn site_with_egress(phase: Option<GridSitePhase>, egress: &str) -> GridSite {
        GridSite {
            metadata: kube::api::ObjectMeta {
                name: Some("test-site".to_owned()),
                generation: Some(1),
                ..Default::default()
            },
            spec: GridSiteSpec {
                grid_network_ref: "test-net".to_owned(),
                egress: Some(EgressConfig {
                    address: egress.to_owned(),
                    tls: EgressTls::default(),
                }),
                region: None,
                sovereignty_zone: None,
                zone: None,
                trust: None,
            },
            status: phase.map(|p| GridSiteStatus {
                phase: p,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn site_events_go_to_the_operator_namespace() {
        let reference = event_reference(&site_no_egress(None), "grid");
        assert_eq!(reference.namespace.as_deref(), Some("grid"), "not default");
        assert_eq!(reference.name.as_deref(), Some("test-site"), "still names the site");
        assert_eq!(reference.kind.as_deref(), Some("GridSite"), "references a GridSite");
    }

    fn site_no_egress(phase: Option<GridSitePhase>) -> GridSite {
        GridSite {
            metadata: kube::api::ObjectMeta {
                name: Some("test-site".to_owned()),
                generation: Some(1),
                ..Default::default()
            },
            spec: GridSiteSpec {
                grid_network_ref: "test-net".to_owned(),
                egress: None,
                region: None,
                sovereignty_zone: None,
                zone: None,
                trust: None,
            },
            status: phase.map(|p| GridSiteStatus {
                phase: p,
                ..Default::default()
            }),
        }
    }

    // -----------------------------------------------------------------------
    // site_phase_next — non-probe phases (outcome = None)
    // -----------------------------------------------------------------------

    #[test]
    fn pending_stays_pending_even_with_egress() {
        let site = site_with_egress(Some(GridSitePhase::Pending), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Pending, &site, None);
        assert_eq!(next, GridSitePhase::Pending);
        assert_eq!(reason, "AwaitingDiscovery");
    }

    #[test]
    fn discovered_with_egress_advances_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:7946");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(
            next,
            GridSitePhase::Connecting,
            "Discovered + gateway address must advance to Connecting"
        );
        assert_eq!(reason, "GatewayAddressKnown");
    }

    #[test]
    fn discovered_without_egress_stays_discovered() {
        let site = site_no_egress(Some(GridSitePhase::Discovered));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(
            next,
            GridSitePhase::Discovered,
            "Discovered + no gateway address must stay Discovered"
        );
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[test]
    fn discovered_with_empty_egress_stays_discovered() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(next, GridSitePhase::Discovered);
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[test]
    fn left_is_preserved() {
        let site = site_no_egress(Some(GridSitePhase::Left));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Left, &site, None);
        assert_eq!(next, GridSitePhase::Left, "Left must be preserved");
        assert_eq!(reason, "Left");
    }

    #[test]
    fn left_remains_terminal() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Left), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(&GridSitePhase::Left, &site, None);
        assert_eq!(phase, GridSitePhase::Left, "Left must remain terminal");
        assert_eq!(reason, "Left");
    }

    #[test]
    fn discovered_with_gateway_address_advances_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:19080");
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(next, GridSitePhase::Connecting);
        assert_eq!(reason, "GatewayAddressKnown");
    }

    #[test]
    fn discovered_without_gateway_address_stays_discovered() {
        let site = site_no_egress(Some(GridSitePhase::Discovered));
        let (next, reason, _msg) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(next, GridSitePhase::Discovered);
        assert_eq!(reason, "GatewayAddressMissing");
    }

    #[test]
    fn phase_reason_codes_are_deterministic() {
        let site = site_with_egress(Some(GridSitePhase::Discovered), "10.0.0.1:8443");
        let (_, r1, _) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        let (_, r2, _) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(r1, r2, "reason must be deterministic for the same inputs");
    }

    // -----------------------------------------------------------------------
    // site_phase_next — probe outcome transitions
    // -----------------------------------------------------------------------

    #[test]
    fn connecting_with_connection_failure_stays_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::ConnectionFailed),
        );
        assert_eq!(
            next,
            GridSitePhase::Connecting,
            "Connecting must stay on connection failure"
        );
        assert_eq!(reason, "ConnectionFailed");
    }

    #[test]
    fn connecting_with_verified_outcome_promotes_to_active() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (next, reason, _msg) =
            site_phase_next(&GridSitePhase::Connecting, &site, Some(&GatewayProbeOutcome::Verified));
        assert_eq!(next, GridSitePhase::Active, "Verified must promote to Active");
        assert_eq!(reason, "TlsVerified");
    }

    #[test]
    fn active_with_connection_failure_demotes_to_unreachable() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::ConnectionFailed),
        );
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Active with connection failure must become Unreachable"
        );
        assert_eq!(reason, "ConnectionFailed");
    }

    #[test]
    fn active_with_address_missing_demotes_to_unreachable() {
        let site = site_no_egress(Some(GridSitePhase::Active));
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::AddressMissing),
        );
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Active without egress cannot remain Active"
        );
        assert_eq!(reason, "EgressMissing");
    }

    #[test]
    fn active_with_verified_stays_active() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) =
            site_phase_next(&GridSitePhase::Active, &site, Some(&GatewayProbeOutcome::Verified));
        assert_eq!(phase, GridSitePhase::Active);
        assert_eq!(reason, "TlsVerified");
    }

    #[test]
    fn unreachable_with_connection_failure_stays_unreachable() {
        let site = site_with_egress(Some(GridSitePhase::Unreachable), "10.0.0.1:8443");
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Unreachable,
            &site,
            Some(&GatewayProbeOutcome::ConnectionFailed),
        );
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Unreachable with failed probe must stay Unreachable"
        );
        assert_eq!(reason, "ConnectionFailed");
    }

    #[test]
    fn unreachable_with_address_missing_stays_unreachable() {
        let site = site_no_egress(Some(GridSitePhase::Unreachable));
        let (next, reason, _msg) = site_phase_next(
            &GridSitePhase::Unreachable,
            &site,
            Some(&GatewayProbeOutcome::AddressMissing),
        );
        assert_eq!(
            next,
            GridSitePhase::Unreachable,
            "Unreachable without gateway must stay Unreachable"
        );
        assert_eq!(reason, "EgressMissing");
    }

    #[test]
    fn unreachable_with_verified_recovers_to_active() {
        let site = site_with_egress(Some(GridSitePhase::Unreachable), "10.0.0.1:8443");
        let (phase, reason, _msg) =
            site_phase_next(&GridSitePhase::Unreachable, &site, Some(&GatewayProbeOutcome::Verified));
        assert_eq!(
            phase,
            GridSitePhase::Active,
            "Unreachable + verified must recover to Active"
        );
        assert_eq!(reason, "TlsVerified");
    }

    // -----------------------------------------------------------------------
    // Trust failure outcomes — always demote to Connecting
    // -----------------------------------------------------------------------

    #[test]
    fn trust_material_missing_stays_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::TrustMaterialMissing),
        );
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "TrustMaterialMissing");
    }

    #[test]
    fn trust_material_invalid_stays_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::TrustMaterialInvalid),
        );
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "TrustMaterialInvalid");
    }

    #[test]
    fn untrusted_issuer_demotes_active_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::UntrustedIssuer),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "trust failure must demote to Connecting"
        );
        assert_eq!(reason, "UntrustedIssuer");
    }

    #[test]
    fn identity_mismatch_demotes_active_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::IdentityMismatch),
        );
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "IdentityMismatch");
    }

    #[test]
    fn certificate_expired_demotes_active_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::CertificateExpired),
        );
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "CertificateExpired");
    }

    #[test]
    fn pin_mismatch_demotes_active_to_connecting() {
        let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) =
            site_phase_next(&GridSitePhase::Active, &site, Some(&GatewayProbeOutcome::PinMismatch));
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "PinMismatch");
    }

    // -----------------------------------------------------------------------
    // Plaintext probe outcomes
    // -----------------------------------------------------------------------

    fn site_with_plaintext_egress(phase: Option<GridSitePhase>, egress: &str) -> GridSite {
        GridSite {
            metadata: kube::api::ObjectMeta {
                name: Some("test-site".to_owned()),
                generation: Some(1),
                ..Default::default()
            },
            spec: GridSiteSpec {
                grid_network_ref: "test-net".to_owned(),
                egress: Some(EgressConfig {
                    address: egress.to_owned(),
                    tls: EgressTls {
                        mode: TlsMode::Plaintext,
                        server_name: None,
                    },
                }),
                region: None,
                sovereignty_zone: None,
                zone: None,
                trust: None,
            },
            status: phase.map(|p| GridSiteStatus {
                phase: p,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn plaintext_connecting_remains_ineligible_when_reachable() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::PlaintextReachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "TCP reachability without verified identity must not promote to Active"
        );
        assert_eq!(reason, condition::PLAINTEXT_INELIGIBLE);
    }

    #[test]
    fn plaintext_connecting_stays_connecting_when_unreachable() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Connecting,
            &site,
            Some(&GatewayProbeOutcome::PlaintextUnreachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "plaintext + unreachable must stay Connecting"
        );
        assert_eq!(reason, condition::PLAINTEXT_INELIGIBLE);
    }

    #[test]
    fn plaintext_active_demotes_when_reachable_without_identity() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::PlaintextReachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "changing an Active site to plaintext must revoke routing eligibility"
        );
        assert_eq!(reason, condition::PLAINTEXT_INELIGIBLE);
    }

    #[test]
    fn plaintext_active_demotes_when_unreachable() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Active,
            &site,
            Some(&GatewayProbeOutcome::PlaintextUnreachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Unreachable,
            "plaintext Active + unreachable must demote"
        );
        assert_eq!(reason, condition::PLAINTEXT_INELIGIBLE);
    }

    #[test]
    fn plaintext_unreachable_moves_to_connecting_when_reachable() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Unreachable), "10.0.0.1:8443");
        let (phase, reason, _msg) = site_phase_next(
            &GridSitePhase::Unreachable,
            &site,
            Some(&GatewayProbeOutcome::PlaintextReachable),
        );
        assert_eq!(
            phase,
            GridSitePhase::Connecting,
            "reachable plaintext cannot recover directly to Active"
        );
        assert_eq!(reason, condition::PLAINTEXT_INELIGIBLE);
    }

    #[test]
    fn plaintext_reports_connected_false_with_plaintext_ineligible() {
        let site = site_with_plaintext_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        for outcome in [
            GatewayProbeOutcome::PlaintextReachable,
            GatewayProbeOutcome::PlaintextUnreachable,
        ] {
            let (phase, reason, message) = site_phase_next(&GridSitePhase::Connecting, &site, Some(&outcome));
            assert_ne!(phase, GridSitePhase::Active, "{outcome:?}");
            let conditions = condition::site_conditions(&phase, &reason, &message);
            let connected = conditions
                .iter()
                .find(|c| c.type_ == condition::CONNECTED)
                .unwrap_or_else(|| std::process::abort());
            assert_eq!(connected.status, condition::ConditionStatus::False, "{outcome:?}");
            assert_eq!(connected.reason, condition::PLAINTEXT_INELIGIBLE, "{outcome:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Message safety — no private material in probe transition messages
    // -----------------------------------------------------------------------

    #[test]
    fn phase_messages_do_not_contain_sentinel_token() {
        let sentinel = "sk-super-secret-token-do-not-emit";
        let non_probe = [GridSitePhase::Pending, GridSitePhase::Discovered, GridSitePhase::Left];
        for phase in &non_probe {
            let site = site_with_egress(Some(phase.clone()), "10.0.0.1:8443");
            let (_, reason, message) = site_phase_next(phase, &site, None);
            assert!(
                !reason.contains(sentinel),
                "reason for {phase:?} must not contain sentinel: {reason}"
            );
            assert!(
                !message.contains(sentinel),
                "message for {phase:?} must not contain sentinel: {message}"
            );
        }
        let outcomes = [
            GatewayProbeOutcome::Verified,
            GatewayProbeOutcome::ConnectionFailed,
            GatewayProbeOutcome::TrustMaterialMissing,
            GatewayProbeOutcome::PinMismatch,
            GatewayProbeOutcome::PlaintextReachable,
        ];
        for outcome in &outcomes {
            let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
            let (_, reason, message) = site_phase_next(&GridSitePhase::Connecting, &site, Some(outcome));
            assert!(!reason.contains(sentinel), "reason must not contain sentinel: {reason}");
            assert!(
                !message.contains(sentinel),
                "message must not contain sentinel: {message}"
            );
        }
    }

    #[test]
    fn probe_outcome_messages_never_contain_pem_or_key_markers() {
        let outcomes = [
            GatewayProbeOutcome::Verified,
            GatewayProbeOutcome::ConnectionFailed,
            GatewayProbeOutcome::ConnectTimeout,
            GatewayProbeOutcome::HandshakeTimeout,
            GatewayProbeOutcome::TrustMaterialMissing,
            GatewayProbeOutcome::TrustMaterialInvalid,
            GatewayProbeOutcome::UntrustedIssuer,
            GatewayProbeOutcome::IdentityMismatch,
            GatewayProbeOutcome::CertificateExpired,
            GatewayProbeOutcome::CertificateNotYetValid,
            GatewayProbeOutcome::PinMismatch,
            GatewayProbeOutcome::PlaintextReachable,
            GatewayProbeOutcome::PlaintextUnreachable,
            GatewayProbeOutcome::AddressMissing,
            GatewayProbeOutcome::TlsProtocolError,
        ];
        for outcome in &outcomes {
            let site = site_with_egress(Some(GridSitePhase::Active), "10.0.0.1:8443");
            let (_, _, message) = site_phase_next(&GridSitePhase::Active, &site, Some(outcome));
            assert!(
                !message.contains("BEGIN CERTIFICATE"),
                "message must not include PEM: {message}"
            );
            assert!(
                !message.contains("PRIVATE KEY"),
                "message must not include key marker: {message}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Helper tests
    // -----------------------------------------------------------------------

    fn tls_network() -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1beta1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": { "tls": { "caSecretRef": { "name": "ca", "namespace": "grid" } } }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    fn site_with_gossiped_egress(address: &str) -> GridSite {
        GridSite {
            status: Some(GridSiteStatus {
                discovered: Some(crate::crd::grid_site::DiscoveredStatus {
                    egress_address: Some(address.to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..site_no_egress(Some(GridSitePhase::Discovered))
        }
    }

    #[test]
    fn egress_tls_mode_follows_the_declared_egress() {
        let network = tls_network();
        let plaintext = site_with_plaintext_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8080");
        assert_eq!(egress_tls_mode(&plaintext, &network), TlsMode::Plaintext);

        let mutual = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8080");
        assert_eq!(egress_tls_mode(&mutual, &network), TlsMode::MutualTls);
    }

    fn network_with_discovery(site_discovery: &serde_json::Value) -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1beta1",
            "kind": "GridNetwork",
            "metadata": { "name": "net" },
            "spec": { "siteDiscovery": site_discovery }
        }))
        .unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn default_egress_tls_is_mutual_tls() {
        let network = network_with_discovery(&serde_json::json!({ "mode": "auto" }));
        assert_eq!(network.spec.site_discovery.default_egress_tls, TlsMode::MutualTls);
    }

    #[test]
    fn a_network_without_tls_refs_no_longer_implies_plaintext() {
        let site = site_with_gossiped_egress("10.0.0.9:8443");
        let no_tls = network_with_discovery(&serde_json::json!({}));
        assert!(no_tls.spec.tls.ca_secret_ref.is_none() && no_tls.spec.tls.site_secret_ref.is_none());
        assert_eq!(egress_tls_mode(&site, &no_tls), TlsMode::MutualTls);
    }

    #[test]
    fn an_auto_site_inherits_an_explicit_plaintext_default() {
        let site = site_with_gossiped_egress("10.0.0.9:8443");
        let network = network_with_discovery(&serde_json::json!({ "mode": "auto", "defaultEgressTls": "plaintext" }));
        assert_eq!(egress_tls_mode(&site, &network), TlsMode::Plaintext);
    }

    #[test]
    fn a_per_site_egress_tls_override_wins_over_the_network_default() {
        let plaintext_net = network_with_discovery(&serde_json::json!({ "defaultEgressTls": "plaintext" }));
        let mutual = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        assert_eq!(egress_tls_mode(&mutual, &plaintext_net), TlsMode::MutualTls);

        let mutual_net = network_with_discovery(&serde_json::json!({ "defaultEgressTls": "mutualTls" }));
        let plaintext = site_with_plaintext_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        assert_eq!(egress_tls_mode(&plaintext, &mutual_net), TlsMode::Plaintext);
    }

    #[test]
    fn a_declared_egress_without_an_address_keeps_its_tls_and_uses_the_gossiped_address() {
        let mut site = site_with_gossiped_egress("10.0.0.9:8443");
        site.spec.egress = serde_json::from_value(serde_json::json!({ "tls": { "mode": "plaintext" } })).ok();
        assert_eq!(egress_address(&site), Some("10.0.0.9:8443"));
        assert_eq!(egress_tls_mode(&site, &tls_network()), TlsMode::Plaintext);
    }

    #[test]
    fn declared_tls_intent_without_any_address_holds_without_probing() {
        let mut site = site_no_egress(Some(GridSitePhase::Discovered));
        site.spec.egress = serde_json::from_value(serde_json::json!({
            "tls": { "mode": "mutualTls", "serverName": "west.grid.internal" }
        }))
        .ok();
        assert_eq!(egress_address(&site), None);
        let (phase, reason, _) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(phase, GridSitePhase::Discovered);
        assert_eq!(reason, "GatewayAddressMissing");
        let (probed, ..) = site_phase_next(&GridSitePhase::Connecting, &site, None);
        assert_ne!(probed, GridSitePhase::Active, "no address can never promote");
    }

    fn site_with_tls(mode: &str, server_name: Option<&str>) -> GridSite {
        let mut site = site_no_egress(Some(GridSitePhase::Connecting));
        let tls = server_name.map_or_else(
            || serde_json::json!({ "mode": mode }),
            |name| serde_json::json!({ "mode": mode, "serverName": name }),
        );
        site.spec.egress = serde_json::from_value(serde_json::json!({ "address": "10.0.0.1:8443", "tls": tls })).ok();
        site
    }

    fn rejection_reason(site: &GridSite, network: &GridNetwork) -> Option<&'static str> {
        site_spec_rejection(site, network).map(|r| r.reason)
    }

    #[test]
    fn mutual_tls_egress_without_a_server_name_defaults_to_the_enrolled_dns_name() {
        let site = site_with_tls("mutualTls", None);
        assert_eq!(rejection_reason(&site, &tls_network()), None);
        let name = site.metadata.name.clone().unwrap_or_default();
        assert_eq!(probe_server_name(&site), format!("{name}.grid.internal"));
        assert_eq!(
            probe_server_name(&site_with_tls("mutualTls", Some(" "))),
            format!("{name}.grid.internal")
        );
        assert_eq!(
            probe_server_name(&site_with_tls("mutualTls", Some("gw.example.com"))),
            "gw.example.com"
        );
    }

    /// A site pinned to two leaf digests, all `a` then all `b`.
    fn twice_pinned_site() -> GridSite {
        site_with_trust(
            Some(GridSitePhase::Connecting),
            "10.0.0.1:8443",
            Some(GridSiteTrustPolicy {
                canonical_fingerprints: Some(vec!["a".repeat(64), "b".repeat(64)]),
            }),
        )
    }

    #[test]
    fn under_pin_the_status_names_the_pinned_digests() {
        let (site, network) = (twice_pinned_site(), tls_network());
        assert_eq!(
            identity_message("TlsVerified", &site, &network).as_deref(),
            Some("TLS handshake verified: chain to the Grid CA and pinned leaf digest aaaaaaaaaaaa or bbbbbbbbbbbb")
        );
        assert_eq!(
            identity_message("PinMismatch", &site, &network).as_deref(),
            Some("server leaf does not match the pinned leaf digest aaaaaaaaaaaa or bbbbbbbbbbbb")
        );
        assert!(
            identity_message("IdentityMismatch", &site, &network).is_none(),
            "pin mode keeps the SAN wording"
        );
        assert!(identity_message("ConnectTimeout", &site, &network).is_none());
    }

    #[test]
    fn under_spiffe_the_status_names_the_spiffe_id_and_no_pin() {
        let site = twice_pinned_site();
        let mut network = tls_network();
        network.spec.peer_trust = serde_json::from_value(serde_json::json!({ "mode": "spiffe" })).ok();
        let verified = identity_message("TlsVerified", &site, &network).unwrap_or_default();
        assert_eq!(
            verified,
            "TLS handshake verified: chain to the Grid CA and SPIFFE ID spiffe://grid.internal/site/test-site"
        );
        assert!(!verified.contains("pin"), "no pin is checked under spiffe");
        assert_eq!(
            identity_message("IdentityMismatch", &site, &network).as_deref(),
            Some(
                "server cert does not match serverName test-site.grid.internal or carry SPIFFE ID \
                 spiffe://grid.internal/site/test-site"
            )
        );
    }

    #[test]
    fn spiffe_peer_trust_follows_the_network_mode() {
        let mut network = tls_network();
        assert!(!spiffe_peer_trust(&network));
        network.spec.peer_trust = serde_json::from_value(serde_json::json!({ "mode": "spiffe" })).ok();
        assert!(spiffe_peer_trust(&network));
    }

    #[test]
    fn plaintext_egress_refuses_a_server_name() {
        assert_eq!(
            rejection_reason(&site_with_tls("plaintext", Some("west.grid.internal")), &tls_network()),
            Some("ServerNameForbidden")
        );
        assert_eq!(
            rejection_reason(&site_with_tls("plaintext", None), &tls_network()),
            None
        );
    }

    #[test]
    fn an_invalid_server_name_is_rejected() {
        assert_eq!(
            rejection_reason(&site_with_tls("mutualTls", Some("not a name")), &tls_network()),
            Some("ServerNameInvalid")
        );
    }

    #[test]
    fn pins_conflict_with_spiffe_peer_trust() {
        let mut network = tls_network();
        network.spec.peer_trust = serde_json::from_value(serde_json::json!({ "mode": "spiffe" })).ok();
        let mut site = site_with_tls("mutualTls", Some("west.grid.internal"));
        site.spec.trust = serde_json::from_value(serde_json::json!({ "canonicalFingerprints": ["a".repeat(64)] })).ok();
        assert_eq!(rejection_reason(&site, &network), Some("TrustConflictsWithPeerTrust"));
        assert_eq!(rejection_reason(&site, &tls_network()), None, "pin mode reads the pins");
    }

    #[test]
    fn a_site_without_declared_egress_is_not_rejected() {
        assert_eq!(rejection_reason(&site_no_egress(None), &tls_network()), None);
    }

    #[test]
    fn a_rejected_site_never_holds_active() {
        assert_eq!(rejected_phase(&GridSitePhase::Active), GridSitePhase::Connecting);
        assert_eq!(rejected_phase(&GridSitePhase::Unreachable), GridSitePhase::Connecting);
        assert_eq!(rejected_phase(&GridSitePhase::Discovered), GridSitePhase::Discovered);
    }

    #[test]
    fn only_a_verified_handshake_promotes_a_site() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        let (phase, ..) = site_phase_next(&GridSitePhase::Connecting, &site, Some(&GatewayProbeOutcome::Verified));
        assert_eq!(phase, GridSitePhase::Active);
    }

    #[test]
    fn gossiped_addresses_must_be_dialable_literal_ips() {
        for refused in [
            "127.0.0.1:8443",
            "[::1]:8443",
            "0.0.0.0:8443",
            "169.254.169.254:80",
            "100.100.100.200:80",
            "[fd00:ec2::254]:80",
            "[fe80::1]:8443",
            "10.0.0.9:0",
            "gateway.example.com:8443",
            "x@169.254.169.254:80",
            "10.0.0.9",
        ] {
            let site = site_with_gossiped_egress(refused);
            assert_eq!(egress_address(&site), None, "{refused}");
            assert!(gossip_address_refused(&site), "{refused}");
            let (phase, reason, _) = site_phase_next(&GridSitePhase::Discovered, &site, None);
            assert_eq!(
                (phase, reason.as_str()),
                (GridSitePhase::Discovered, "GossipedAddressRefused"),
                "{refused}"
            );
        }
        for allowed in ["10.0.0.9:8443", "[2001:db8::1]:8443"] {
            assert_eq!(egress_address(&site_with_gossiped_egress(allowed)), Some(allowed));
        }
    }

    #[test]
    fn a_declared_egress_may_name_a_host() {
        let site = site_with_egress(Some(GridSitePhase::Connecting), "gateway.example.com:8443");
        assert_eq!(egress_address(&site), Some("gateway.example.com:8443"));
        assert!(!gossip_address_refused(&site));
    }

    fn unreachable_for(minutes: i64) -> (GridSiteStatus, time::OffsetDateTime) {
        let since = time::OffsetDateTime::UNIX_EPOCH + time::Duration::days(1);
        let stamp = since
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| std::process::abort());
        let status = GridSiteStatus {
            phase: GridSitePhase::Unreachable,
            conditions: condition::reconcile_conditions(
                &[],
                condition::site_conditions(&GridSitePhase::Unreachable, "ConnectTimeout", ""),
                1,
                &stamp,
            ),
            ..Default::default()
        };
        (status, since + time::Duration::minutes(minutes))
    }

    #[test]
    fn an_unreachable_site_backs_off_between_bounds() {
        let (least, most) = UNREACHABLE_BACKOFF;
        let (fresh, at_start) = unreachable_for(0);
        let first = requeue_after(&GridSitePhase::Unreachable, Some(&fresh), at_start);
        assert!(first >= least && first <= least.mul_f64(1.2), "{first:?}");
        let (long_down, an_hour_in) = unreachable_for(60);
        let later = requeue_after(&GridSitePhase::Unreachable, Some(&long_down), an_hour_in);
        assert!(later >= most && later <= most.mul_f64(1.2), "{later:?}");
        let (mid, minutes_in) = unreachable_for(4);
        let middle = requeue_after(&GridSitePhase::Unreachable, Some(&mid), minutes_in);
        assert!(
            middle >= Duration::from_secs(120) && middle <= Duration::from_secs(144),
            "{middle:?}"
        );
    }

    #[test]
    fn a_reachable_site_keeps_the_usual_interval() {
        let (down, now) = unreachable_for(60);
        assert_eq!(
            requeue_after(&GridSitePhase::Active, Some(&down), now),
            REQUEUE_INTERVAL
        );
        assert_eq!(requeue_after(&GridSitePhase::Connecting, None, now), REQUEUE_INTERVAL);
    }

    #[test]
    fn the_declared_egress_wins_over_the_gossiped_one() {
        let mut site = site_with_egress(Some(GridSitePhase::Connecting), "10.0.0.1:8443");
        site.status = site_with_gossiped_egress("10.0.0.9:8443").status;
        assert_eq!(egress_address(&site), Some("10.0.0.1:8443"));
        assert_eq!(
            egress_address(&site_with_gossiped_egress("10.0.0.9:8443")),
            Some("10.0.0.9:8443")
        );
        assert_eq!(egress_address(&site_no_egress(None)), None);
    }

    #[test]
    fn a_gossiped_address_advances_a_discovered_site() {
        let site = site_with_gossiped_egress("10.0.0.9:8443");
        let (phase, reason, _) = site_phase_next(&GridSitePhase::Discovered, &site, None);
        assert_eq!(phase, GridSitePhase::Connecting);
        assert_eq!(reason, "GatewayAddressKnown");
    }

    #[test]
    fn needs_probe_for_active_phases() {
        assert!(needs_probe(&GridSitePhase::Connecting), "Connecting needs probe");
        assert!(needs_probe(&GridSitePhase::Active), "Active needs probe");
        assert!(needs_probe(&GridSitePhase::Unreachable), "Unreachable needs probe");
        assert!(!needs_probe(&GridSitePhase::Pending), "Pending does not need probe");
        assert!(
            !needs_probe(&GridSitePhase::Discovered),
            "Discovered does not need probe"
        );
        assert!(!needs_probe(&GridSitePhase::Left), "Left does not need probe");
    }

    // -----------------------------------------------------------------------
    // Pin resolution — rotation and legacy compatibility
    // -----------------------------------------------------------------------

    use crate::crd::grid_site::GridSiteTrustPolicy;

    fn site_with_trust(phase: Option<GridSitePhase>, egress: &str, trust: Option<GridSiteTrustPolicy>) -> GridSite {
        GridSite {
            metadata: kube::api::ObjectMeta {
                name: Some("test-site".to_owned()),
                generation: Some(1),
                ..Default::default()
            },
            spec: GridSiteSpec {
                grid_network_ref: "test-net".to_owned(),
                egress: Some(EgressConfig {
                    address: egress.to_owned(),
                    tls: EgressTls::default(),
                }),
                region: None,
                sovereignty_zone: None,
                zone: None,
                trust,
            },
            status: phase.map(|p| GridSiteStatus {
                phase: p,
                ..Default::default()
            }),
        }
    }

    fn valid_pin() -> String {
        "a".repeat(64)
    }

    fn valid_pin_2() -> String {
        "b".repeat(64)
    }

    #[test]
    fn resolve_pins_no_trust_policy_fails_closed() {
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", None);
        assert_eq!(
            resolve_pins(&site),
            Err(GatewayProbeOutcome::TrustMaterialMissing),
            "no trust policy must remain in bootstrap"
        );
    }

    #[test]
    fn resolve_pins_single_canonical_pin() {
        let trust = GridSiteTrustPolicy {
            canonical_fingerprints: Some(vec![valid_pin()]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let pins = resolve_pins(&site).unwrap_or_else(|_| std::process::abort());
        assert_eq!(pins.len(), 1, "single canonical pin");
    }

    #[test]
    fn resolve_pins_two_canonical_pins_for_rotation() {
        let trust = GridSiteTrustPolicy {
            canonical_fingerprints: Some(vec![valid_pin(), valid_pin_2()]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let pins = resolve_pins(&site).unwrap_or_else(|_| std::process::abort());
        assert_eq!(pins.len(), 2, "two canonical pins for rotation overlap");
    }

    #[test]
    fn resolve_pins_three_pins_rejected() {
        let trust = GridSiteTrustPolicy {
            canonical_fingerprints: Some(vec![valid_pin(), valid_pin_2(), "c".repeat(64)]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let result = resolve_pins(&site);
        assert_eq!(
            result,
            Err(GatewayProbeOutcome::TrustMaterialInvalid),
            "three pins must be rejected"
        );
    }

    #[test]
    fn resolve_pins_empty_pin_list_rejected() {
        let trust = GridSiteTrustPolicy {
            canonical_fingerprints: Some(Vec::new()),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        assert_eq!(
            resolve_pins(&site),
            Err(GatewayProbeOutcome::TrustMaterialInvalid),
            "present but empty pin policy is invalid"
        );
    }

    #[test]
    fn resolve_pins_invalid_pin_format_rejected() {
        let trust = GridSiteTrustPolicy {
            canonical_fingerprints: Some(vec!["not-a-valid-hex-fingerprint".to_owned()]),
        };
        let site = site_with_trust(Some(GridSitePhase::Connecting), "10.0.0.1:8443", Some(trust));
        let result = resolve_pins(&site);
        assert_eq!(
            result,
            Err(GatewayProbeOutcome::TrustMaterialInvalid),
            "invalid pin format must be rejected"
        );
    }

    // -----------------------------------------------------------------------
    // Event helpers
    // -----------------------------------------------------------------------

    #[test]
    fn event_type_tls_verified_is_normal() {
        assert!(matches!(event_type_for_reason("TlsVerified"), EventType::Normal));
    }

    #[test]
    fn event_type_awaiting_discovery_is_normal() {
        assert!(matches!(event_type_for_reason("AwaitingDiscovery"), EventType::Normal));
    }

    #[test]
    fn event_type_gateway_address_known_is_normal() {
        assert!(matches!(
            event_type_for_reason("GatewayAddressKnown"),
            EventType::Normal
        ));
    }

    #[test]
    fn event_type_left_is_normal() {
        assert!(matches!(event_type_for_reason("Left"), EventType::Normal));
    }

    #[test]
    fn event_type_pin_mismatch_is_warning() {
        assert!(matches!(event_type_for_reason("PinMismatch"), EventType::Warning));
    }

    #[test]
    fn event_type_connection_failed_is_warning() {
        assert!(matches!(event_type_for_reason("ConnectionFailed"), EventType::Warning));
    }

    #[test]
    fn event_type_certificate_expired_is_warning() {
        assert!(matches!(
            event_type_for_reason("CertificateExpired"),
            EventType::Warning
        ));
    }

    #[test]
    fn event_type_trust_material_missing_is_warning() {
        assert!(matches!(
            event_type_for_reason("TrustMaterialMissing"),
            EventType::Warning
        ));
    }

    #[test]
    fn event_type_identity_mismatch_is_warning() {
        assert!(matches!(event_type_for_reason("IdentityMismatch"), EventType::Warning));
    }

    #[test]
    fn truncate_event_note_short_message_unchanged() {
        let msg = "TLS handshake verified";
        assert_eq!(truncate_event_note(msg), msg);
    }

    #[test]
    fn truncate_event_note_long_message_truncated() {
        let msg = "a".repeat(300);
        let result = truncate_event_note(&msg);
        assert!(result.ends_with("..."), "long message should end with ...");
        assert!(
            result.chars().count() <= crate::resources::gateway_probe::MAX_STATUS_MESSAGE_LEN,
            "truncated message must not exceed MAX_STATUS_MESSAGE_LEN"
        );
    }
}
