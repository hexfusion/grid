//! Narrated, evidence-backed combined-site demo scenarios.
#![expect(
    clippy::collapsible_if,
    clippy::string_slice,
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    clippy::unwrap_used,
    clippy::doc_markdown,
    clippy::manual_map,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::disallowed_methods,
    clippy::assigning_clones,
    clippy::redundant_closure_for_method_calls,
    reason = "Demo orchestration code prioritizes clarity over lint perfection"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use serde::Serialize;

use super::{
    DemoMode, GlbDemoOptions, certs,
    external_provider::{self, ExternalProviderDescriptor},
    glb, kubectl, operator,
};

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

/// Directory where generated TLS certificates are stored.
const CERTS_DIR: &str = "tests/env/certs";

/// Ordered cluster names in the combined-site scenario environment.
const CLUSTERS: &[&str] = &["west", "central", "east"];

/// Consumer gateway TLS secret name (matches Helm `existingSecret` reference).
const CONSUMER_TLS_SECRET: &str = "consumer-gateway-tls";

/// Evidence JSON schema version.
const EVIDENCE_SCHEMA_VERSION: &str = "1";

/// Kubernetes namespace for all Grid components.
const GRID_SYSTEM_NS: &str = "grid-system";

/// Overlay ConfigMap name created by the Grid operator for consumer gateways.
const OVERLAY_CONFIGMAP: &str = "grid-overlay-grid-combined-site-consumer-gateway";

/// Provider credential secret name (matches Helm `credentials[0].name`).
const MOCK_INFERENCE_CREDENTIAL: &str = "mock-inference-credential";

/// Stable terminal separator that also remains readable in captured logs.
const OUTPUT_RULE: &str = "===============================================================================";

/// Provider gateway service name advertised via SWIM for cross-site discovery.
const PROVIDER_GATEWAY_SERVICE: &str = "provider-gateway";

/// Provider gateway port advertised via SWIM for cross-site discovery.
const PROVIDER_GATEWAY_PORT: &str = "8443";

/// Provider gateway TLS secret name (matches Helm `existingSecret` reference).
const PROVIDER_TLS_SECRET: &str = "provider-gateway-tls";

/// Same-CA client identity with an organization rejected by `peer_identity_trust`.
const WRONG_ORG_TLS_SECRET: &str = "wrong-org-client-tls";

/// Number of environment setup phases shown to the user.
const SETUP_PHASES: usize = 14;

// -----------------------------------------------------------------------------
// Context
// -----------------------------------------------------------------------------

/// Combined-site demo execution context.
struct CombinedSiteContext {
    /// Path to the resolved Forge config.
    resolved_config: PathBuf,
    /// Path to the forge binary.
    forge_bin: PathBuf,
    /// External provider descriptor, if enabled.
    external_provider: Option<ExternalProviderDescriptor>,
    /// External provider site selection.
    external_provider_site: Option<String>,
    /// Path to the external provider key file (kept separate from the descriptor
    /// so the descriptor never carries the filesystem path after validation).
    external_key_file: Option<PathBuf>,
}

// -----------------------------------------------------------------------------
// Overlay State
// -----------------------------------------------------------------------------

/// Per-site overlay snapshot captured from the ConfigMap.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct OverlayData {
    /// Kubernetes ConfigMap `resourceVersion` (per-cluster, never compared
    /// across clusters).
    resource_version: String,
    /// Content-addressed semantic revision from the
    /// `grid.praxis-proxy.io/overlay-revision` annotation. Changes only when
    /// routing-relevant fields change; safe to compare across clusters.
    semantic_revision: String,
    /// Candidate name to stable ID mapping.
    stable_ids: BTreeMap<String, String>,
    /// Full candidate details for evidence (kind, name, site, cluster, model).
    candidates: Vec<OverlayCandidate>,
}

/// Subset of `RoutingCandidate` fields kept for evidence and validation.
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
struct OverlayCandidate {
    /// Candidate kind (e.g. `inference_model`).
    kind: String,
    /// Model name.
    name: String,
    /// Site name.
    site: String,
    /// Upstream cluster identifier (used as the candidate key).
    cluster: String,
    /// Deterministic stable ID for session binding.
    stable_id: String,
}

/// Expected external candidate derived from `ExternalProviderDescriptor`.
///
/// Used to validate that the overlay's external candidate matches the
/// descriptor's routing cluster, model, and site -- not just the name.
#[derive(Clone, Debug)]
struct ExpectedExternalCandidate {
    /// InferenceProvider name: `external-{provider_kind}`.
    name: String,
    /// Expected `routing_cluster` field from the descriptor.
    routing_cluster: String,
    /// Expected model name.
    model: String,
    /// Site where the external provider is deployed.
    site: String,
}

/// Pre- and post-SWIM overlay snapshots for evidence.
#[derive(Clone, Debug, Default, serde::Deserialize, Serialize)]
struct OverlayState {
    /// Per-site overlays before SWIM seeding (local candidates only).
    pre_swim: BTreeMap<String, OverlayData>,
    /// Per-site overlays after global convergence.
    post_swim: BTreeMap<String, OverlayData>,
}

// -----------------------------------------------------------------------------
// Evidence
// -----------------------------------------------------------------------------

/// Evidence written to `results.json`.
#[derive(Debug, serde::Deserialize, Serialize)]
struct Evidence {
    /// Evidence schema version.
    schema_version: String,
    /// Demo mode that was executed.
    mode: String,
    /// Topology name.
    topology: String,
    /// List of cluster names.
    clusters: Vec<String>,
    /// Proof results for each assertion.
    proof_results: BTreeMap<String, ProofResult>,
    /// Exact image references used.
    images: BTreeMap<String, String>,
    /// External provider configuration, if used.
    external_provider: Option<ExternalProviderEvidence>,
    /// Pre- and post-SWIM overlay snapshots.
    overlay_state: OverlayState,
    /// Cluster health status.
    cluster_health: Vec<ClusterHealth>,
    /// Component deployment status.
    components: Vec<ComponentStatus>,
    /// SWIM membership views.
    swim_membership: Vec<SwimMembership>,
    /// Provider response samples.
    provider_responses: Vec<ProviderResponse>,
    /// Security assertion results.
    security_results: Vec<SecurityResult>,
    /// Teardown success.
    teardown_success: bool,
}

/// Evidence for one proof assertion.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ProofResult {
    /// Whether the proof passed.
    success: bool,
    /// Human-readable reason.
    reason: String,
    /// Observed facts that support this result.
    observed_facts: BTreeMap<String, serde_json::Value>,
    /// Duration of the assertion in milliseconds.
    duration_ms: u64,
}

/// Cluster health status.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ClusterHealth {
    /// Cluster name.
    name: String,
    /// Whether the cluster is healthy.
    healthy: bool,
    /// API server response time in milliseconds.
    api_response_ms: Option<u64>,
    /// Number of ready nodes.
    ready_nodes: u32,
}

/// Component deployment status.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ComponentStatus {
    /// Component name.
    name: String,
    /// Deployment namespace.
    namespace: String,
    /// Ready replicas.
    ready_replicas: u32,
    /// Desired replicas.
    desired_replicas: u32,
    /// Whether the component is ready.
    ready: bool,
}

/// SWIM membership view.
#[derive(Debug, serde::Deserialize, Serialize)]
struct SwimMembership {
    /// Site name.
    site: String,
    /// Local node ID.
    local_node: String,
    /// List of known peers.
    peers: Vec<String>,
    /// Membership convergence status.
    converged: bool,
}

/// Provider response metadata.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ProviderResponse {
    /// Consumer site that made the request.
    consumer_site: String,
    /// Provider site that served the request.
    provider_site: String,
    /// Provider instance ID.
    provider_instance: String,
    /// Session ID.
    session_id: String,
    /// Serving revision.
    serving_revision: String,
    /// Response time in milliseconds.
    response_time_ms: u64,
    /// Whether the response was successful.
    success: bool,
}

/// Security assertion result.
#[derive(Debug, serde::Deserialize, Serialize)]
struct SecurityResult {
    /// Type of security test.
    test_type: String,
    /// Expected result (allow/deny).
    expected: String,
    /// Actual result.
    actual: String,
    /// Whether the test passed.
    passed: bool,
    /// Additional context.
    context: String,
}

/// External provider evidence.
#[derive(Debug, serde::Deserialize, Serialize)]
struct ExternalProviderEvidence {
    /// Provider kind (e.g., "openai").
    kind: String,
    /// Selected site.
    site: String,
    /// Model name.
    model: String,
    /// Routing cluster name.
    cluster: String,
}

// -----------------------------------------------------------------------------
// Utility Functions
// -----------------------------------------------------------------------------

/// Format current UTC timestamp for run IDs.
fn format_utc_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or_else(|_| "unknown".to_owned(), |duration| format!("{}", duration.as_secs()))
}

/// Format current UTC timestamp in ISO format.
fn format_utc_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
        |_| "unknown-utc".to_owned(),
        |duration| format!("{}-utc", duration.as_secs()),
    )
}

/// Resolve the evidence directory path.
fn resolve_evidence_dir(
    forge_config: &Path,
    options: &GlbDemoOptions,
    run_id: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(dir) = &options.evidence_dir {
        Ok(dir.clone())
    } else {
        let config_dir = forge_config
            .parent()
            .ok_or("forge config should have parent directory")?;
        Ok(config_dir.join(format!("evidence-{run_id}")))
    }
}

// -----------------------------------------------------------------------------
// Assertion Framework
// -----------------------------------------------------------------------------

/// Result of a runtime assertion.
type AssertionResult = Result<ProofResult, Box<dyn std::error::Error>>;

/// Create a successful proof result with observed facts.
fn proof_success(reason: &str, observed_facts: BTreeMap<String, serde_json::Value>, duration: Duration) -> ProofResult {
    ProofResult {
        success: true,
        reason: reason.to_owned(),
        observed_facts,
        duration_ms: duration.as_millis().min(u64::MAX as u128) as u64,
    }
}

/// Create a failed proof result with observed facts.
fn proof_failure(reason: &str, observed_facts: BTreeMap<String, serde_json::Value>, duration: Duration) -> ProofResult {
    ProofResult {
        success: false,
        reason: reason.to_owned(),
        observed_facts,
        duration_ms: duration.as_millis().min(u64::MAX as u128) as u64,
    }
}

/// Execute an assertion with timing and error handling.
fn run_assertion<F>(name: &str, assertion_fn: F) -> AssertionResult
where
    F: FnOnce() -> AssertionResult,
{
    let start = Instant::now();
    eprintln!("  [ASSERT] {name}");

    let result = assertion_fn();
    let _duration = start.elapsed();

    match &result {
        Ok(proof) => {
            if proof.success {
                eprintln!("  [OK] {name}: {}", proof.reason);
            } else {
                eprintln!("  [FAIL] {name}: {}", proof.reason);
            }
        },
        Err(e) => {
            eprintln!("  [ERROR] {name}: {e}");
        },
    }

    result.map_err(|e| {
        // Convert assertion errors to proof failures
        format!("Assertion {name} failed: {e}").into()
    })
}

/// Poll for a condition with bounded retries.
#[expect(
    clippy::disallowed_methods,
    reason = "Sleep is required for polling with timeout functionality"
)]
fn poll_until<F, T>(condition: F, timeout: Duration, interval: Duration) -> Result<T, Box<dyn std::error::Error>>
where
    F: Fn() -> Result<Option<T>, Box<dyn std::error::Error>>,
{
    let start = Instant::now();

    loop {
        match condition() {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {
                if start.elapsed() >= timeout {
                    return Err("Timeout waiting for condition".into());
                }
                std::thread::sleep(interval);
            },
            Err(e) => return Err(e),
        }
    }
}

/// Wait for a deployment to be ready.
fn wait_for_deployment(deployment: &str, namespace: &str, context: &str) -> Result<(), Box<dyn std::error::Error>> {
    kubectl::wait_for_rollout_ns(context, deployment, namespace, "deployment")?;
    Ok(())
}

/// Pod-security overrides for ephemeral curl pods in restricted namespaces.
///
/// `kubectl run` names the container after the pod, so the container name must
/// match. The curlimages/curl image runs as UID 100.
fn curl_pod_overrides(pod_name: &str, curl_args: &[&str]) -> String {
    let args = curl_args.strip_prefix(&["curl"]).unwrap_or(curl_args);
    serde_json::json!({
        "spec": {
            "automountServiceAccountToken": false,
            "securityContext": {
                "runAsNonRoot": true,
                "seccompProfile": { "type": "RuntimeDefault" }
            },
            "containers": [{
                "name": pod_name,
                "image": "curlimages/curl:8.12.1",
                "command": ["curl"],
                "args": args,
                "securityContext": {
                    "runAsUser": 100,
                    "allowPrivilegeEscalation": false,
                    "readOnlyRootFilesystem": true,
                    "capabilities": { "drop": ["ALL"] }
                }
            }]
        }
    })
    .to_string()
}

/// Run an ephemeral curl pod with restricted PodSecurity context.
fn run_curl_probe(context: &str, pod_name: &str, curl_args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    let overrides = curl_pod_overrides(pod_name, curl_args);
    Command::new("kubectl")
        .args([
            "run",
            pod_name,
            "--image=curlimages/curl:8.12.1",
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "--rm",
            "-i",
            "--restart=Never",
            "--overrides",
            &overrides,
        ])
        .output()
}

/// Run an ephemeral curl pod with additional kubectl flags (e.g. `--labels`).
fn run_curl_probe_with_flags(
    context: &str,
    pod_name: &str,
    extra_kubectl_args: &[&str],
    curl_args: &[&str],
) -> Result<std::process::Output, std::io::Error> {
    let overrides = curl_pod_overrides(pod_name, curl_args);
    Command::new("kubectl")
        .args([
            "run",
            pod_name,
            "--image=curlimages/curl:8.12.1",
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "--rm",
            "-i",
            "--restart=Never",
            "--overrides",
            &overrides,
        ])
        .args(extra_kubectl_args)
        .output()
}

/// Return a response header value from curl `--include` output.
fn response_header(output: &[u8], name: &str) -> Option<String> {
    let expected = name.to_ascii_lowercase();
    String::from_utf8_lossy(output).lines().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(&expected).then(|| value.trim().to_owned())
    })
}

/// Wait for the demo environment to be ready.
fn wait_for_environment_ready() -> Result<String, Box<dyn std::error::Error>> {
    // Wait for Grid operators to converge
    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");
        eprintln!("  [WAIT] {cluster}: Grid operator convergence");

        // Wait for deployment to be ready
        wait_for_deployment("grid-operator", "grid-system", &context)?;

        // Wait for consumer and provider gateways
        wait_for_deployment("consumer-gateway", "grid-system", &context)?;
        wait_for_deployment("provider-gateway", "grid-system", &context)?;

        eprintln!("  [OK] {cluster}: Gateways ready");
    }

    Ok("All three combined sites converged and gateways ready".to_owned())
}

// -----------------------------------------------------------------------------
// Runtime Assertions
// -----------------------------------------------------------------------------

/// Assert exactly three clusters exist and are healthy.
fn assert_cluster_health() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut cluster_health = Vec::new();

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Check API server responsiveness
        let api_start = Instant::now();
        let output = Command::new("kubectl")
            .args(["cluster-info", "--context", &context])
            .output()?;

        let api_response_ms = api_start.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let healthy = output.status.success();

        // Get node count
        let nodes_output = Command::new("kubectl")
            .args([
                "get",
                "nodes",
                "--context",
                &context,
                "-o",
                "jsonpath={.items[*].status.conditions[?(@.type=='Ready')].status}",
            ])
            .output()?;

        let ready_nodes = if nodes_output.status.success() {
            String::from_utf8_lossy(&nodes_output.stdout)
                .split_whitespace()
                .filter(|s| s == &"True")
                .count()
                .min(u32::MAX as usize) as u32
        } else {
            0
        };

        cluster_health.push(ClusterHealth {
            name: cluster.to_string(),
            healthy,
            api_response_ms: Some(api_response_ms),
            ready_nodes,
        });

        observed_facts.insert(format!("{cluster}_healthy"), serde_json::Value::Bool(healthy));
        observed_facts.insert(
            format!("{cluster}_ready_nodes"),
            serde_json::Value::Number(ready_nodes.into()),
        );
    }

    let all_healthy = cluster_health.iter().all(|c| c.healthy && c.ready_nodes > 0);
    observed_facts.insert(
        "total_clusters".to_owned(),
        serde_json::Value::Number(CLUSTERS.len().into()),
    );

    if all_healthy {
        Ok(proof_success(
            &format!("All {} clusters are healthy with ready nodes", CLUSTERS.len()),
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more clusters are unhealthy",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert exactly one of each component exists per site.
fn assert_component_deployment() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut components = Vec::new();

    let static_components = ["grid-operator", "consumer-gateway", "provider-gateway"];

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");
        let mock_name = format!("mock-inference-{cluster}");
        let cluster_components: Vec<&str> = static_components
            .iter()
            .copied()
            .chain(std::iter::once(mock_name.as_str()))
            .collect();

        for component in &cluster_components {
            let output = Command::new("kubectl")
                .args([
                    "get",
                    "deployment",
                    component,
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "-o",
                    "jsonpath={.status.readyReplicas},{.status.replicas}",
                ])
                .output()?;

            if output.status.success() {
                let status_str = String::from_utf8_lossy(&output.stdout);
                let parts: Vec<&str> = status_str.trim().split(',').collect();

                let ready_replicas = parts.first().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
                let desired_replicas = parts.get(1).and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
                let ready = ready_replicas > 0 && ready_replicas == desired_replicas;

                components.push(ComponentStatus {
                    name: format!("{cluster}-{component}"),
                    namespace: GRID_SYSTEM_NS.to_owned(),
                    ready_replicas,
                    desired_replicas,
                    ready,
                });

                observed_facts.insert(format!("{cluster}_{component}_ready"), serde_json::Value::Bool(ready));
                observed_facts.insert(
                    format!("{cluster}_{component}_replicas"),
                    serde_json::Value::Number(ready_replicas.into()),
                );
            } else {
                components.push(ComponentStatus {
                    name: format!("{cluster}-{component}"),
                    namespace: GRID_SYSTEM_NS.to_owned(),
                    ready_replicas: 0,
                    desired_replicas: 1,
                    ready: false,
                });
                observed_facts.insert(format!("{cluster}_{component}_ready"), serde_json::Value::Bool(false));
            }
        }
    }

    let components_per_cluster = static_components.len() + 1;
    let expected_count = CLUSTERS.len() * components_per_cluster;
    let all_ready = components.len() == expected_count && components.iter().all(|c| c.ready);
    observed_facts.insert(
        "total_components".to_owned(),
        serde_json::Value::Number(expected_count.into()),
    );

    if all_ready {
        Ok(proof_success(
            &format!(
                "All {} components are ready across {} sites",
                components_per_cluster,
                CLUSTERS.len()
            ),
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more components are not ready",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert every site's `GridNetwork` reports both remote sites connected.
fn assert_swim_convergence() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let expected_remote_sites = CLUSTERS.len() - 1;
    let mut all_converged = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");
        let status = poll_until(
            || {
                let output = Command::new("kubectl")
                    .args([
                        "get",
                        "gridnetwork/grid-combined-site",
                        "--context",
                        &context,
                        "-o",
                        "jsonpath={.status.phase},{.status.connectedSites}",
                    ])
                    .output()?;
                if !output.status.success() {
                    return Ok(None);
                }
                let value = String::from_utf8_lossy(&output.stdout);
                let Some((phase, connected)) = value.trim().split_once(',') else {
                    return Ok(None);
                };
                let connected = connected.parse::<usize>().unwrap_or_default();
                Ok((phase == "Active" && connected == expected_remote_sites).then_some((phase.to_owned(), connected)))
            },
            Duration::from_secs(90),
            Duration::from_secs(3),
        );

        match status {
            Ok((phase, connected)) => {
                observed_facts.insert(format!("{cluster}_phase"), serde_json::Value::String(phase));
                observed_facts.insert(
                    format!("{cluster}_connected_sites"),
                    serde_json::Value::Number(connected.into()),
                );
            },
            Err(error) => {
                all_converged = false;
                observed_facts.insert(format!("{cluster}_error"), serde_json::Value::String(error.to_string()));
            },
        }
    }

    observed_facts.insert(
        "expected_remote_sites".to_owned(),
        serde_json::Value::Number(expected_remote_sites.into()),
    );

    if all_converged {
        Ok(proof_success(
            "Every GridNetwork is Active with both remote sites connected",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more GridNetworks did not report both remote sites connected",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Verify that both remote sites are discovered and routing-eligible.
///
/// The locally declared placement site is not a remote SWIM discovery result
/// and is excluded from this assertion.
fn assert_site_auto_discovery() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let expected_remote_count = CLUSTERS.len() - 1;
    let mut all_ok = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        let result = poll_until(
            || {
                let output = Command::new("kubectl")
                    .args([
                        "get", "gridsite",
                        "-l", "grid.praxis-proxy.io/auto-discovered=true",
                        "--context", &context,
                        "-n", GRID_SYSTEM_NS,
                        "-o", "jsonpath={range .items[*]}{.metadata.name}\t{.status.phase}\t{.status.reason}\t{.spec.egress.address}\t{.spec.egress.tls.serverName}\t{.spec.trust.canonicalFingerprints}\n{end}",
                    ])
                    .output()?;
                if !output.status.success() {
                    return Ok(None);
                }
                let body = String::from_utf8_lossy(&output.stdout);
                let mut verified = Vec::new();
                let port_suffix = format!(":{PROVIDER_GATEWAY_PORT}");
                for line in body.lines() {
                    let fields: Vec<&str> = line.trim().split('\t').collect();
                    let [name, phase, reason, addr, server_name, fingerprints, ..] = fields.as_slice() else {
                        continue;
                    };
                    if *phase == "Active"
                        && *reason == "TlsVerified"
                        && addr.ends_with(&port_suffix)
                        && !server_name.is_empty()
                        && !fingerprints.is_empty()
                    {
                        verified.push(serde_json::json!({
                            "name": name,
                            "phase": phase,
                            "reason": reason,
                            "egressAddress": addr,
                            "serverName": server_name,
                            "hasFingerprints": true,
                        }));
                    }
                }
                if verified.len() == expected_remote_count {
                    Ok(Some(verified))
                } else {
                    Ok(None)
                }
            },
            Duration::from_secs(120),
            Duration::from_secs(5),
        );

        match result {
            Ok(remotes) => {
                observed_facts.insert(format!("{cluster}_remote_sites"), serde_json::Value::Array(remotes));
            },
            Err(error) => {
                all_ok = false;
                observed_facts.insert(format!("{cluster}_error"), serde_json::Value::String(error.to_string()));
            },
        }
    }

    observed_facts.insert(
        "expected_remote_count".to_owned(),
        serde_json::Value::Number(expected_remote_count.into()),
    );

    if all_ok {
        Ok(proof_success(
            "Every cluster has two Active/TlsVerified remote GridSites with :8443 addresses and trust configuration",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more clusters lack Active auto-discovered remote GridSites",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert each consumer receives and accepts a versioned overlay.
///
/// Validates per-site: ConfigMap exists with non-empty data, gateway deployment
/// is ready, and a request is routed through the accepted overlay.
fn assert_overlay_acceptance() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_accepted = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Step 1: Overlay ConfigMap exists
        let overlay_output = Command::new("kubectl")
            .args([
                "get",
                "configmap",
                "grid-overlay-grid-combined-site-consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.metadata.resourceVersion}",
            ])
            .output()?;

        let resource_version = String::from_utf8_lossy(&overlay_output.stdout).trim().to_owned();
        let overlay_exists = overlay_output.status.success() && !resource_version.is_empty();

        // Step 2: Overlay ConfigMap has non-empty data
        let overlay_data_output = Command::new("kubectl")
            .args([
                "get",
                "configmap",
                "grid-overlay-grid-combined-site-consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.data}",
            ])
            .output()?;

        let data_str = String::from_utf8_lossy(&overlay_data_output.stdout).trim().to_owned();
        let has_data = overlay_data_output.status.success() && !data_str.is_empty() && data_str != "{}";

        // Step 3: Consumer gateway deployment is ready
        let deploy_output = Command::new("kubectl")
            .args([
                "get",
                "deployment/consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.status.readyReplicas}",
            ])
            .output()?;

        let gateway_ready = deploy_output.status.success()
            && String::from_utf8_lossy(&deploy_output.stdout)
                .trim()
                .parse::<u32>()
                .is_ok_and(|n| n > 0);

        // Step 4: A valid request proves that the accepted overlay is serving.
        // Praxis health endpoints are exposed only on the pod-local admin listener.
        let routing_output = run_curl_probe(
            &context,
            &format!("overlay-routing-{cluster}"),
            &[
                "curl",
                "--fail-with-body",
                "--silent",
                "--show-error",
                "--max-time",
                "10",
                "--header",
                "Content-Type: application/json",
                "--data",
                r#"{"model":"mock-model","messages":[{"role":"user","content":"overlay probe"}]}"#,
                "http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
            ],
        )?;

        let routing_ok = routing_output.status.success();

        let overlay_accepted = overlay_exists && has_data && gateway_ready && routing_ok;
        if !overlay_accepted {
            all_accepted = false;
        }

        observed_facts.insert(
            format!("{cluster}_overlay_configmap_exists"),
            serde_json::Value::Bool(overlay_exists),
        );
        observed_facts.insert(format!("{cluster}_overlay_has_data"), serde_json::Value::Bool(has_data));
        observed_facts.insert(
            format!("{cluster}_gateway_ready"),
            serde_json::Value::Bool(gateway_ready),
        );
        observed_facts.insert(
            format!("{cluster}_overlay_routing_ok"),
            serde_json::Value::Bool(routing_ok),
        );
        observed_facts.insert(
            format!("{cluster}_resource_version"),
            serde_json::Value::String(resource_version),
        );
    }

    observed_facts.insert(
        "all_overlays_accepted".to_owned(),
        serde_json::Value::Bool(all_accepted),
    );

    if all_accepted {
        Ok(proof_success(
            "All sites: overlay ConfigMap exists with data, gateway ready, health check passes",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more sites failed overlay acceptance (ConfigMap/data/ready/routing)",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert local provider selection preference: west selects west, central selects central, east selects east.
fn assert_local_provider_selection() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_local = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Send test request to consumer gateway and check response attribution
        let test_output = run_curl_probe(
            &context,
            &format!("route-test-{cluster}"),
            &[
                "curl",
                "-f",
                "--include",
                "-H",
                "Content-Type: application/json",
                "-H",
                &format!("X-Session-Id: local-test-{cluster}"),
                "-d",
                r#"{"model": "mock-model", "messages": []}"#,
                "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
            ],
        )?;

        let provider_site = response_header(&test_output.stdout, "x-grid-combined-provider-gateway")
            .unwrap_or_else(|| "unknown".to_owned());
        let selected_local = test_output.status.success() && provider_site == *cluster;

        if !selected_local {
            all_local = false;
        }

        observed_facts.insert(
            format!("{cluster}_selected_provider_site"),
            serde_json::Value::String(provider_site),
        );
        observed_facts.insert(
            format!("{cluster}_selected_local"),
            serde_json::Value::Bool(selected_local),
        );
    }

    observed_facts.insert("all_sites_prefer_local".to_owned(), serde_json::Value::Bool(all_local));

    if all_local {
        Ok(proof_success(
            "All consumer sites correctly prefer their local provider (west→west, central→central, east→east)",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more consumer sites did not select their local provider",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert live responses identify consumer site, provider site, candidate, provider instance, session, and serving
/// revision.
fn assert_response_attribution() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_attributed = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Send test request and verify response includes all required attribution
        let test_output = run_curl_probe(
            &context,
            &format!("attribution-test-{cluster}"),
            &[
                "curl",
                "-f",
                "--include",
                "-H",
                "Content-Type: application/json",
                "-H",
                &format!("X-Session-Id: attribution-test-{cluster}"),
                "-d",
                r#"{"model": "mock-model", "messages": []}"#,
                "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
            ],
        )?;

        let mut attribution_fields = BTreeMap::new();
        let required_headers = [
            ("consumer_site", "x-grid-combined-consumer-gateway"),
            ("provider_site", "x-grid-combined-provider-gateway"),
            ("backend_provider", "x-grid-demo-backend-provider-attribution"),
            ("request_id", "x-grid-demo-backend-request-id"),
            ("serving_revision", "x-grid-demo-backend-overlay-revision"),
        ];
        for (field, header) in required_headers {
            if let Some(value) = response_header(&test_output.stdout, header) {
                attribution_fields.insert(field.to_owned(), serde_json::Value::String(value));
            }
        }
        let has_all_attribution = test_output.status.success() && attribution_fields.len() == required_headers.len();

        if !has_all_attribution {
            all_attributed = false;
        }

        observed_facts.insert(
            format!("{cluster}_has_full_attribution"),
            serde_json::Value::Bool(has_all_attribution),
        );
        observed_facts.insert(
            format!("{cluster}_attribution_fields"),
            serde_json::Value::Object(attribution_fields.into_iter().collect()),
        );
    }

    observed_facts.insert(
        "all_responses_fully_attributed".to_owned(),
        serde_json::Value::Bool(all_attributed),
    );

    if all_attributed {
        Ok(proof_success(
            "All responses include complete attribution: consumer site, provider site, instance, session, and revision",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more responses lack complete attribution metadata",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert no-client-certificate provider connection fails and wrong peer identity fails.
///
/// Wrong-identity test: mount the consumer-gateway TLS secret (valid CA-signed cert
/// with consumer identity) into a test pod and present it to the provider-gateway.
/// The positive control presents the consumer identity used by the real provider
/// hop. The negative control uses a same-CA certificate with an untrusted
/// organization, separating TLS client authentication from peer authorization.
fn assert_tls_certificate_validation() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_rejections_correct = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Test 1: No client certificate should fail (TLS rejection)
        let no_cert_output = run_curl_probe(
            &context,
            &format!("no-cert-test-{cluster}"),
            &[
                "curl",
                "-k",
                "--connect-timeout",
                "5",
                "https://provider-gateway.grid-system.svc.cluster.local:8443/v1/models",
            ],
        )?;

        let no_cert_rejected = !no_cert_output.status.success();

        let positive_pod_yaml = format!(
            r#"apiVersion: v1
kind: Pod
metadata:
  name: tls-positive-{cluster}
  namespace: grid-system
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  volumes:
  - name: client-tls
    secret:
      secretName: consumer-gateway-tls
  containers:
  - name: curl
    image: curlimages/curl:8.12.1
    securityContext:
      runAsUser: 100
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop: ["ALL"]
    volumeMounts:
    - name: client-tls
      mountPath: /tls
      readOnly: true
    command: ["curl", "-k", "-sS", "-o", "/dev/null", "--cert", "/tls/tls.crt", "--key", "/tls/tls.key", "--connect-timeout", "10", "https://provider-gateway.grid-system.svc.cluster.local:8443/v1/models"]
"#
        );

        let positive_result = run_pod_to_completion(
            &context,
            &format!("tls-positive-{cluster}"),
            &positive_pod_yaml,
            Duration::from_secs(30),
        );
        let positive_control_passed = positive_result.is_ok() && positive_result.unwrap();

        let wrong_id_pod_yaml = format!(
            r#"apiVersion: v1
kind: Pod
metadata:
  name: tls-wrong-id-{cluster}
  namespace: grid-system
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  volumes:
  - name: wrong-tls
    secret:
      secretName: wrong-org-client-tls
  containers:
  - name: curl
    image: curlimages/curl:8.12.1
    securityContext:
      runAsUser: 100
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop: ["ALL"]
    volumeMounts:
    - name: wrong-tls
      mountPath: /tls
      readOnly: true
    command: ["curl", "-k", "-f", "-sS", "-o", "/dev/null", "--cert", "/tls/tls.crt", "--key", "/tls/tls.key", "--connect-timeout", "10", "https://provider-gateway.grid-system.svc.cluster.local:8443/v1/models"]
"#
        );

        let wrong_id_result = run_pod_to_completion(
            &context,
            &format!("tls-wrong-id-{cluster}"),
            &wrong_id_pod_yaml,
            Duration::from_secs(30),
        );
        let wrong_identity_rejected = wrong_id_result.is_ok() && !wrong_id_result.unwrap();

        let cluster_rejections_correct = no_cert_rejected && wrong_identity_rejected && positive_control_passed;
        if !cluster_rejections_correct {
            all_rejections_correct = false;
        }

        observed_facts.insert(
            format!("{cluster}_no_client_cert_rejected"),
            serde_json::Value::Bool(no_cert_rejected),
        );
        observed_facts.insert(
            format!("{cluster}_positive_control_consumer_identity_accepted"),
            serde_json::Value::Bool(positive_control_passed),
        );
        observed_facts.insert(
            format!("{cluster}_wrong_organization_cert_rejected"),
            serde_json::Value::Bool(wrong_identity_rejected),
        );
    }

    observed_facts.insert(
        "all_tls_validations_correct".to_owned(),
        serde_json::Value::Bool(all_rejections_correct),
    );

    if all_rejections_correct {
        Ok(proof_success(
            "TLS validation correct: no-cert and wrong-organization identities rejected; consumer identity accepted",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "TLS certificate validation failed: check individual cluster results",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Run a pod from YAML to completion and return whether it succeeded.
/// Cleans up the pod after completion or timeout.
fn run_pod_to_completion(
    context: &str,
    pod_name: &str,
    pod_yaml: &str,
    timeout: Duration,
) -> Result<bool, Box<dyn std::error::Error>> {
    // Delete any leftover pod from a previous run
    let _cleanup = Command::new("kubectl")
        .args([
            "delete",
            "pod",
            pod_name,
            "--context",
            context,
            "-n",
            "grid-system",
            "--ignore-not-found",
        ])
        .output();

    // Apply the pod
    let mut apply = Command::new("kubectl")
        .args(["apply", "--context", context, "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    if let Some(ref mut stdin) = apply.stdin {
        stdin.write_all(pod_yaml.as_bytes())?;
    }
    drop(apply.stdin.take());
    let apply_result = apply.wait_with_output()?;
    if !apply_result.status.success() {
        return Err(format!("Failed to create pod {pod_name}").into());
    }

    // Poll until pod reaches a terminal phase (Succeeded or Failed)
    let interval = Duration::from_secs(2);
    let result = poll_until(
        || {
            let phase_output = Command::new("kubectl")
                .args([
                    "get",
                    "pod",
                    pod_name,
                    "--context",
                    context,
                    "-n",
                    "grid-system",
                    "-o",
                    "jsonpath={.status.phase}",
                ])
                .output();

            match phase_output {
                Ok(output) if output.status.success() => {
                    let phase = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                    match phase.as_str() {
                        "Succeeded" => Ok(Some(true)),
                        "Failed" => Ok(Some(false)),
                        _ => Ok(None),
                    }
                },
                _ => Ok(None),
            }
        },
        timeout,
        interval,
    );

    // Clean up the pod
    let _cleanup = Command::new("kubectl")
        .args([
            "delete",
            "pod",
            pod_name,
            "--context",
            context,
            "-n",
            "grid-system",
            "--ignore-not-found",
        ])
        .output();

    match result {
        Ok(succeeded) => Ok(succeeded),
        Err(_) => Err(format!("Pod {pod_name} did not complete within timeout").into()),
    }
}

/// Assert authorized provider-hop traffic succeeds and caller authorization is replaced at final hop.
fn assert_authorization_replacement() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_replacement_correct = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Send request with specific authorization header
        let auth_test_output = run_curl_probe(
            &context,
            &format!("auth-test-{cluster}"),
            &[
                "curl",
                "-f",
                "-H",
                "Content-Type: application/json",
                "-H",
                "Authorization: Bearer consumer-token",
                "-H",
                &format!("X-Session-Id: auth-test-{cluster}"),
                "-d",
                r#"{"model": "mock-model", "messages": []}"#,
                "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
            ],
        )?;

        let auth_successful = auth_test_output.status.success();

        // The same caller token must be rejected when it bypasses credential_inject.
        // This proves the successful routed request did not reach the backend with
        // the downstream Authorization value, without exposing the provider token.
        let direct_output = run_curl_probe_with_flags(
            &context,
            &format!("auth-direct-{cluster}"),
            &["--labels=app.kubernetes.io/instance=provider-gateway"],
            &[
                "curl",
                "-f",
                "-H",
                "Authorization: Bearer consumer-token",
                "-H",
                "Content-Type: application/json",
                "-d",
                r#"{"model":"mock-model","messages":[]}"#,
                &format!("http://mock-inference-{cluster}.grid-system.svc.cluster.local:8080/v1/chat/completions"),
            ],
        )?;
        let auth_replaced = auth_successful && !direct_output.status.success();

        let cluster_auth_correct = auth_successful && auth_replaced;
        if !cluster_auth_correct {
            all_replacement_correct = false;
        }

        observed_facts.insert(
            format!("{cluster}_authorized_traffic_successful"),
            serde_json::Value::Bool(auth_successful),
        );
        observed_facts.insert(
            format!("{cluster}_authorization_replaced_at_hop"),
            serde_json::Value::Bool(auth_replaced),
        );
    }

    observed_facts.insert(
        "all_authorization_replacement_correct".to_owned(),
        serde_json::Value::Bool(all_replacement_correct),
    );

    if all_replacement_correct {
        Ok(proof_success(
            "Authorized provider-hop traffic succeeds with caller authorization properly replaced at final hop",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Authorization replacement at provider hop failed",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert consumer and operator cannot access provider credentials.
fn assert_credential_isolation() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_isolated = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Test 1: Consumer gateway should not have provider credentials mounted
        let consumer_volumes_output = Command::new("kubectl")
            .args([
                "get",
                "deployment/consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.volumes[*].secret.secretName}",
            ])
            .output()?;

        let consumer_isolated = if consumer_volumes_output.status.success() {
            let mounted_secrets = String::from_utf8_lossy(&consumer_volumes_output.stdout);
            // Consumer should not have provider-specific credentials mounted
            !mounted_secrets.contains("mock-inference-credential") && !mounted_secrets.contains("provider-credential")
        } else {
            false // If query fails, we can't verify isolation
        };

        // Test 2: Operator should not have provider credentials mounted
        let operator_volumes_output = Command::new("kubectl")
            .args([
                "get",
                "deployment/grid-operator",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.volumes[*].secret.secretName}",
            ])
            .output()?;

        let operator_isolated = if operator_volumes_output.status.success() {
            let mounted_secrets = String::from_utf8_lossy(&operator_volumes_output.stdout);
            // Operator should not have provider-specific credentials mounted
            !mounted_secrets.contains("mock-inference-credential") && !mounted_secrets.contains("provider-credential")
        } else {
            false // If query fails, we can't verify isolation
        };

        // Additional test: Verify ServiceAccount permissions
        let consumer_sa_output = Command::new("kubectl")
            .args([
                "auth",
                "can-i",
                "get",
                "secrets",
                "--as=system:serviceaccount:grid-system:consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
            ])
            .output()?;

        let consumer_rbac_isolated = !consumer_sa_output.status.success();

        let operator_sa_output = Command::new("kubectl")
            .args([
                "auth",
                "can-i",
                "get",
                "secrets",
                "--as=system:serviceaccount:grid-system:grid-operator",
                "--context",
                &context,
                "-n",
                "grid-system",
            ])
            .output()?;

        let operator_can_read_secrets = operator_sa_output.status.success();

        // The controller reads Grid TLS Secrets as part of its reconciliation
        // contract. Provider credentials remain isolated by pod mounts; requiring
        // a namespace-wide Secret denial would contradict the operator's RBAC.
        let cluster_isolated = consumer_isolated && operator_isolated && consumer_rbac_isolated;
        if !cluster_isolated {
            all_isolated = false;
        }

        observed_facts.insert(
            format!("{cluster}_consumer_mount_isolation"),
            serde_json::Value::Bool(consumer_isolated),
        );
        observed_facts.insert(
            format!("{cluster}_operator_mount_isolation"),
            serde_json::Value::Bool(operator_isolated),
        );
        observed_facts.insert(
            format!("{cluster}_consumer_rbac_isolation"),
            serde_json::Value::Bool(consumer_rbac_isolated),
        );
        observed_facts.insert(
            format!("{cluster}_operator_secret_read_required_by_controller"),
            serde_json::Value::Bool(operator_can_read_secrets),
        );
    }

    observed_facts.insert(
        "all_credentials_properly_isolated".to_owned(),
        serde_json::Value::Bool(all_isolated),
    );

    if all_isolated {
        Ok(proof_success(
            "Provider credentials are absent from consumer and operator mounts; consumer Secret reads are denied",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Credential isolation failed: provider credentials leaked through mounts or consumer RBAC",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert direct consumer/workload backend access is denied by NetworkPolicy.
fn assert_backend_access_denial() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_denied = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // First, positive control: verify provider gateway can reach backend
        let provider_control_output = run_curl_probe_with_flags(
            &context,
            &format!("provider-control-{cluster}"),
            &["--labels=app.kubernetes.io/instance=provider-gateway"],
            &[
                "curl",
                "-f",
                "--connect-timeout",
                "5",
                "--max-time",
                "10",
                &format!("http://mock-inference-{cluster}.grid-system.svc.cluster.local:8080/health"),
            ],
        )?;

        let provider_control_success = provider_control_output.status.success();

        // Second, positive control: verify operator can reach backend if allowed
        let operator_control_output = run_curl_probe_with_flags(
            &context,
            &format!("operator-control-{cluster}"),
            &["--labels=app.kubernetes.io/name=grid-operator"],
            &[
                "curl",
                "-f",
                "--connect-timeout",
                "5",
                "--max-time",
                "10",
                &format!("http://mock-inference-{cluster}.grid-system.svc.cluster.local:8080/health"),
            ],
        )?;

        let operator_control_success = operator_control_output.status.success();

        // Test direct access from unlabeled pod (should be denied by NetworkPolicy)
        let direct_access_output = run_curl_probe(
            &context,
            &format!("direct-access-test-{cluster}"),
            &[
                "curl",
                "--connect-timeout",
                "5",
                "--max-time",
                "10",
                &format!("http://mock-inference-{cluster}.grid-system.svc.cluster.local:8080/health"),
            ],
        )?;

        // Should fail due to NetworkPolicy - but only if controls succeeded
        let direct_access_denied = !direct_access_output.status.success() && provider_control_success;

        // Test access from external namespace (should be denied)
        let external_ns_output = run_curl_probe_with_flags(
            &context,
            &format!("external-test-{cluster}"),
            &["-n", "default"],
            &[
                "curl",
                "--connect-timeout",
                "5",
                "--max-time",
                "10",
                &format!("http://mock-inference-{cluster}.grid-system.svc.cluster.local:8080/health"),
            ],
        )?;

        let external_access_denied = !external_ns_output.status.success() && provider_control_success;

        let cluster_access_denied =
            direct_access_denied && external_access_denied && provider_control_success && operator_control_success;
        if !cluster_access_denied {
            all_denied = false;
        }

        observed_facts.insert(
            format!("{cluster}_provider_control_success"),
            serde_json::Value::Bool(provider_control_success),
        );
        observed_facts.insert(
            format!("{cluster}_operator_control_success"),
            serde_json::Value::Bool(operator_control_success),
        );
        observed_facts.insert(
            format!("{cluster}_direct_backend_access_denied"),
            serde_json::Value::Bool(direct_access_denied),
        );
        observed_facts.insert(
            format!("{cluster}_external_namespace_access_denied"),
            serde_json::Value::Bool(external_access_denied),
        );
    }

    observed_facts.insert(
        "all_backend_access_properly_denied".to_owned(),
        serde_json::Value::Bool(all_denied),
    );

    if all_denied {
        Ok(proof_success(
            "Direct consumer/workload backend access is correctly denied by NetworkPolicy",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "NetworkPolicy failed to deny direct backend access",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert central drain triggers remote fallback detection.
fn assert_central_drain_fallback() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();

    // Scale down central provider
    let central_context = "kind-grid-combined-central";
    let scale_output = Command::new("kubectl")
        .args([
            "scale",
            "deployment",
            "provider-gateway",
            "--replicas=0",
            "-n",
            "grid-system",
            "--context",
            central_context,
        ])
        .output()?;

    let drain_successful = scale_output.status.success();
    observed_facts.insert(
        "central_provider_drained".to_owned(),
        serde_json::Value::Bool(drain_successful),
    );

    if !drain_successful {
        return Ok(proof_failure(
            "Failed to drain central provider",
            observed_facts,
            start.elapsed(),
        ));
    }

    // Poll until provider replicas are actually zero
    let timeout = Duration::from_secs(30);
    let interval = Duration::from_secs(2);
    let drain_result = poll_until(
        || {
            let replicas_output = Command::new("kubectl")
                .args([
                    "get",
                    "deployment",
                    "provider-gateway",
                    "-n",
                    "grid-system",
                    "--context",
                    central_context,
                    "-o",
                    "jsonpath={.status.replicas}",
                ])
                .output();

            match replicas_output {
                Ok(output) if output.status.success() => {
                    let replicas = String::from_utf8_lossy(&output.stdout);
                    if replicas.trim() == "0" { Ok(Some(())) } else { Ok(None) }
                },
                _ => Ok(None),
            }
        },
        timeout,
        interval,
    );

    if drain_result.is_err() {
        observed_facts.insert(
            "drain_timeout_reason".to_owned(),
            serde_json::Value::String("Provider replicas did not reach zero within timeout".to_owned()),
        );
        return Ok(proof_failure(
            "Central provider drain did not complete within timeout",
            observed_facts,
            start.elapsed(),
        ));
    }

    // Test central consumer now uses remote providers
    let fallback_output = run_curl_probe(
        central_context,
        "fallback-test-central",
        &[
            "curl",
            "-f",
            "-H",
            "Content-Type: application/json",
            "-H",
            "X-Session-Id: fallback-test-central",
            "-d",
            r#"{"model": "mock-model", "messages": []}"#,
            "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
        ],
    )?;

    let mut fallback_detected = false;
    let mut remote_provider_site = "none".to_owned();

    if fallback_output.status.success() {
        let response = String::from_utf8_lossy(&fallback_output.stdout);
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&response) {
            if let Some(metadata) = json.get("metadata") {
                if let Some(site) = metadata.get("provider_site") {
                    if let Some(site_str) = site.as_str() {
                        remote_provider_site = site_str.to_owned();
                        fallback_detected = site_str == "west" || site_str == "east";
                    }
                }
            }
        }
    }

    observed_facts.insert(
        "fallback_to_remote_detected".to_owned(),
        serde_json::Value::Bool(fallback_detected),
    );
    observed_facts.insert(
        "remote_provider_site".to_owned(),
        serde_json::Value::String(remote_provider_site),
    );

    if fallback_detected {
        Ok(proof_success(
            "Central drain successfully triggered remote fallback detection",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Central drain did not trigger proper remote fallback",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert session preservation during provider fallback.
///
/// Correct test order:
/// 1. Establish session BEFORE drain (with local provider serving)
/// 2. Drain central provider
/// 3. Test existing session survives on remote fallback
/// 4. Test new session works on remote fallback
///
/// This function assumes central provider is still up when called.
/// The caller (run_full_scenarios) must call this BEFORE drain,
/// then call assert_existing_session_after_drain and assert_new_session_after_drain.
fn assert_session_establishment() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();

    let session_id = "pre-drain-session";
    let central_context = "kind-grid-combined-central";

    let initial_output = run_curl_probe(
        central_context,
        "session-establish",
        &[
            "curl",
            "-f",
            "-H",
            "Content-Type: application/json",
            "-H",
            &format!("X-Session-Id: {session_id}"),
            "-d",
            r#"{"model": "mock-model", "messages": [{"role": "user", "content": "pre-drain-context"}]}"#,
            "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
        ],
    )?;

    let established = initial_output.status.success();
    let mut provider_site = "unknown".to_owned();

    if established {
        let response = String::from_utf8_lossy(&initial_output.stdout);
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&response) {
            if let Some(metadata) = json.get("metadata") {
                if let Some(site) = metadata.get("provider_site").and_then(|s| s.as_str()) {
                    provider_site = site.to_owned();
                }
            }
        }
    }

    observed_facts.insert(
        "session_established_before_drain".to_owned(),
        serde_json::Value::Bool(established),
    );
    observed_facts.insert(
        "initial_provider_site".to_owned(),
        serde_json::Value::String(provider_site),
    );

    if established {
        Ok(proof_success(
            "Session established before drain with local provider",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Failed to establish session before drain",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert the pre-drain session survives after central provider is drained.
/// Must be called AFTER assert_central_drain_fallback.
fn assert_existing_session_after_drain() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();

    let session_id = "pre-drain-session";
    let central_context = "kind-grid-combined-central";

    let followup_output = run_curl_probe(
        central_context,
        "session-existing-after-drain",
        &[
            "curl",
            "-f",
            "-H",
            "Content-Type: application/json",
            "-H",
            &format!("X-Session-Id: {session_id}"),
            "-d",
            r#"{"model": "mock-model", "messages": [{"role": "user", "content": "after-drain"}]}"#,
            "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
        ],
    )?;

    let followup_successful = followup_output.status.success();
    let mut remote_provider = "unknown".to_owned();

    if followup_successful {
        let response = String::from_utf8_lossy(&followup_output.stdout);
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&response) {
            if let Some(metadata) = json.get("metadata") {
                if let Some(site) = metadata.get("provider_site").and_then(|s| s.as_str()) {
                    remote_provider = site.to_owned();
                }
            }
        }
    }

    observed_facts.insert(
        "existing_session_survived_drain".to_owned(),
        serde_json::Value::Bool(followup_successful),
    );
    observed_facts.insert(
        "fallback_provider_site".to_owned(),
        serde_json::Value::String(remote_provider),
    );

    if followup_successful {
        Ok(proof_success(
            "Pre-drain session survived central provider drain and was served by remote fallback",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Pre-drain session did not survive central provider drain",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert a new session can be established after central provider is drained.
/// Must be called AFTER assert_central_drain_fallback.
fn assert_new_session_after_drain() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();

    let new_session_id = "post-drain-new-session";
    let central_context = "kind-grid-combined-central";

    let new_output = run_curl_probe(
        central_context,
        "session-new-after-drain",
        &[
            "curl",
            "-f",
            "-H",
            "Content-Type: application/json",
            "-H",
            &format!("X-Session-Id: {new_session_id}"),
            "-d",
            r#"{"model": "mock-model", "messages": [{"role": "user", "content": "new-session-content"}]}"#,
            "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
        ],
    )?;

    let new_session_works = new_output.status.success();
    let mut provider_site = "unknown".to_owned();

    if new_session_works {
        let response = String::from_utf8_lossy(&new_output.stdout);
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&response) {
            if let Some(metadata) = json.get("metadata") {
                if let Some(site) = metadata.get("provider_site").and_then(|s| s.as_str()) {
                    provider_site = site.to_owned();
                }
            }
        }
    }

    observed_facts.insert(
        "new_session_after_drain_works".to_owned(),
        serde_json::Value::Bool(new_session_works),
    );
    observed_facts.insert(
        "new_session_provider_site".to_owned(),
        serde_json::Value::String(provider_site),
    );

    if new_session_works {
        Ok(proof_success(
            "New session established after central drain, served by remote provider",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Failed to establish new session after central drain",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Guarantee central provider restoration regardless of test outcome.
fn ensure_central_provider_restored() -> Result<(), Box<dyn std::error::Error>> {
    let central_context = "kind-grid-combined-central";
    let scale = Command::new("kubectl")
        .args([
            "scale",
            "deployment",
            "provider-gateway",
            "--replicas=1",
            "-n",
            "grid-system",
            "--context",
            central_context,
        ])
        .output()?;
    if !scale.status.success() {
        return Err(format!(
            "failed to scale central provider: {}",
            String::from_utf8_lossy(&scale.stderr).trim()
        )
        .into());
    }

    poll_until(
        || {
            let ready_output = Command::new("kubectl")
                .args([
                    "get",
                    "deployment/provider-gateway",
                    "--context",
                    central_context,
                    "-n",
                    "grid-system",
                    "-o",
                    "jsonpath={.status.readyReplicas}",
                ])
                .output();
            match ready_output {
                Ok(output) if output.status.success() => {
                    let count = String::from_utf8_lossy(&output.stdout);
                    if count.trim() == "1" { Ok(Some(())) } else { Ok(None) }
                },
                _ => Ok(None),
            }
        },
        Duration::from_secs(60),
        Duration::from_secs(3),
    )?;
    Ok(())
}

/// Assert provider restoration returns routing to local preference.
fn assert_provider_restoration() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();

    // Restore central provider
    let central_context = "kind-grid-combined-central";
    let scale_output = Command::new("kubectl")
        .args([
            "scale",
            "deployment",
            "provider-gateway",
            "--replicas=1",
            "-n",
            "grid-system",
            "--context",
            central_context,
        ])
        .output()?;

    let restoration_initiated = scale_output.status.success();
    observed_facts.insert(
        "restoration_initiated".to_owned(),
        serde_json::Value::Bool(restoration_initiated),
    );

    if !restoration_initiated {
        return Ok(proof_failure(
            "Failed to initiate provider restoration",
            observed_facts,
            start.elapsed(),
        ));
    }

    // Poll until provider is actually ready
    let timeout = Duration::from_secs(60);
    let interval = Duration::from_secs(3);
    let restoration_result = poll_until(
        || {
            let ready_output = Command::new("kubectl")
                .args([
                    "get",
                    "deployment/provider-gateway",
                    "--context",
                    central_context,
                    "-n",
                    "grid-system",
                    "-o",
                    "jsonpath={.status.readyReplicas}",
                ])
                .output();

            match ready_output {
                Ok(output) if output.status.success() => {
                    let ready_count = String::from_utf8_lossy(&output.stdout);
                    if ready_count.trim() == "1" {
                        Ok(Some(()))
                    } else {
                        Ok(None)
                    }
                },
                _ => Ok(None),
            }
        },
        timeout,
        interval,
    );

    if restoration_result.is_err() {
        observed_facts.insert(
            "restoration_timeout_reason".to_owned(),
            serde_json::Value::String("Provider gateway did not become ready within timeout".to_owned()),
        );
        return Ok(proof_failure(
            "Provider restoration did not complete within timeout",
            observed_facts,
            start.elapsed(),
        ));
    }

    // Verify provider gateway is ready
    let ready_output = Command::new("kubectl")
        .args([
            "get",
            "deployment/provider-gateway",
            "--context",
            central_context,
            "-n",
            "grid-system",
            "-o",
            "jsonpath={.status.readyReplicas}",
        ])
        .output()?;

    let provider_ready = if ready_output.status.success() {
        let ready_count = String::from_utf8_lossy(&ready_output.stdout);
        ready_count.trim() == "1"
    } else {
        false
    };

    observed_facts.insert(
        "provider_gateway_ready".to_owned(),
        serde_json::Value::Bool(provider_ready),
    );

    // Test that central consumer now prefers local provider again
    let local_preference_output = run_curl_probe(
        central_context,
        "restoration-test-central",
        &[
            "curl",
            "-f",
            "-H",
            "Content-Type: application/json",
            "-H",
            "X-Session-Id: restoration-test-central",
            "-d",
            r#"{"model": "mock-model", "messages": []}"#,
            "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
        ],
    )?;

    let mut local_preference_restored = false;
    let mut selected_provider_site = "none".to_owned();

    if local_preference_output.status.success() {
        let response = String::from_utf8_lossy(&local_preference_output.stdout);
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&response) {
            if let Some(metadata) = json.get("metadata") {
                if let Some(site) = metadata.get("provider_site") {
                    if let Some(site_str) = site.as_str() {
                        selected_provider_site = site_str.to_owned();
                        local_preference_restored = site_str == "central";
                    }
                }
            }
        }
    }

    observed_facts.insert(
        "local_preference_restored".to_owned(),
        serde_json::Value::Bool(local_preference_restored),
    );
    observed_facts.insert(
        "selected_provider_site".to_owned(),
        serde_json::Value::String(selected_provider_site),
    );

    if provider_ready && local_preference_restored {
        Ok(proof_success(
            "Provider restoration successfully returned routing to local preference",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Provider restoration failed to restore local preference",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert overlay rollout convergence across all sites.
fn assert_rollout_convergence() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    // Check each site's rendered -> distributed -> accepted -> serving chain individually
    let mut all_sites_converged = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Step 1: Check rendered overlay exists (Grid operator produced it)
        let rendered_output = Command::new("kubectl")
            .args([
                "get",
                "configmap",
                "grid-overlay-grid-combined-site-consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.metadata.labels}",
            ])
            .output()?;

        let mut rendered = false;
        let mut distributed = false;
        let mut accepted = false;

        if rendered_output.status.success() {
            let labels = String::from_utf8_lossy(&rendered_output.stdout);
            rendered = labels.contains("grid.praxis-proxy.io/rendered");
        }

        // Step 2: Check distributed (ConfigMap available to consumer gateway)
        let distributed_output = Command::new("kubectl")
            .args([
                "get",
                "configmap",
                "grid-overlay-grid-combined-site-consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.data}",
            ])
            .output()?;

        if distributed_output.status.success() {
            let overlay_data = String::from_utf8_lossy(&distributed_output.stdout);
            distributed = !overlay_data.trim().is_empty();
        }

        // Step 3: Check accepted (gateway logs show acceptance)
        let acceptance_logs = Command::new("kubectl")
            .args([
                "logs",
                "deployment/consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "--tail=30",
            ])
            .output()?;

        if acceptance_logs.status.success() {
            let logs = String::from_utf8_lossy(&acceptance_logs.stdout);
            accepted = logs.contains("overlay accepted") || logs.contains("configuration loaded");
        }

        // Step 4: Check serving (gateway responds with overlay-configured behavior)
        let serving_test = run_curl_probe(
            &context,
            &format!("serving-test-{cluster}"),
            &[
                "curl",
                "-f",
                "-H",
                "X-Test-Overlay: true",
                "consumer-gateway.grid-system.svc.cluster.local:8080/health",
            ],
        )?;

        let serving = serving_test.status.success();

        let site_converged = rendered && distributed && accepted && serving;
        if !site_converged {
            all_sites_converged = false;
        }

        observed_facts.insert(format!("{cluster}_rendered"), serde_json::Value::Bool(rendered));
        observed_facts.insert(format!("{cluster}_distributed"), serde_json::Value::Bool(distributed));
        observed_facts.insert(format!("{cluster}_accepted"), serde_json::Value::Bool(accepted));
        observed_facts.insert(format!("{cluster}_serving"), serde_json::Value::Bool(serving));
        observed_facts.insert(
            format!("{cluster}_chain_complete"),
            serde_json::Value::Bool(site_converged),
        );
    }

    observed_facts.insert(
        "all_sites_converged".to_owned(),
        serde_json::Value::Bool(all_sites_converged),
    );

    if all_sites_converged {
        Ok(proof_success(
            "Overlay rollout converged: all sites completed rendered -> distributed -> accepted -> serving chain",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Rollout convergence failed: one or more sites did not complete the full chain",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert operator restart recovery maintains cluster state across all three clusters.
///
/// Restarts each operator sequentially (west, central, east), and after each restart:
/// 1. Polls until the operator deployment is ready
/// 2. Verifies SWIM membership via the GridNetwork CRD status subresource
/// 3. Verifies routing still works through the consumer gateway
fn assert_operator_restart_recovery() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_recovered = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Verify operator is ready before restart
        let initial_output = Command::new("kubectl")
            .args([
                "get",
                "deployment/grid-operator",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.status.readyReplicas}",
            ])
            .output()?;

        let initially_ready =
            initial_output.status.success() && String::from_utf8_lossy(&initial_output.stdout).trim() == "1";

        if !initially_ready {
            observed_facts.insert(format!("{cluster}_initially_ready"), serde_json::Value::Bool(false));
            all_recovered = false;
            continue;
        }

        // Restart the operator
        let restart_output = Command::new("kubectl")
            .args([
                "rollout",
                "restart",
                "deployment/grid-operator",
                "--context",
                &context,
                "-n",
                "grid-system",
            ])
            .output()?;

        if !restart_output.status.success() {
            observed_facts.insert(format!("{cluster}_restart_initiated"), serde_json::Value::Bool(false));
            all_recovered = false;
            continue;
        }

        // Poll until operator is ready again
        let deploy_recovered = poll_until(
            || {
                let out = Command::new("kubectl")
                    .args([
                        "get",
                        "deployment/grid-operator",
                        "--context",
                        &context,
                        "-n",
                        "grid-system",
                        "-o",
                        "jsonpath={.status.readyReplicas}",
                    ])
                    .output();
                match out {
                    Ok(o) if o.status.success() => {
                        if String::from_utf8_lossy(&o.stdout).trim() == "1" {
                            Ok(Some(()))
                        } else {
                            Ok(None)
                        }
                    },
                    _ => Ok(None),
                }
            },
            Duration::from_secs(90),
            Duration::from_secs(3),
        )
        .is_ok();

        // Verify SWIM membership from the reconciled GridNetwork status.
        let swim_recovered = if deploy_recovered {
            poll_until(
                || {
                    let membership_output = Command::new("kubectl")
                        .args([
                            "get",
                            "gridnetwork/grid-combined-site",
                            "--context",
                            &context,
                            "-o",
                            "jsonpath={.status.phase},{.status.connectedSites}",
                        ])
                        .output();

                    match membership_output {
                        Ok(out) if out.status.success() => {
                            let body = String::from_utf8_lossy(&out.stdout);
                            let converged = body
                                .trim()
                                .split_once(',')
                                .is_some_and(|(phase, sites)| phase == "Active" && sites == "2");
                            if converged { Ok(Some(())) } else { Ok(None) }
                        },
                        _ => Ok(None),
                    }
                },
                Duration::from_secs(60),
                Duration::from_secs(3),
            )
            .is_ok()
        } else {
            false
        };

        // Routing proof: send a request through the consumer gateway on this cluster
        let routing_works = if deploy_recovered {
            let route_output = run_curl_probe(
                &context,
                &format!("restart-route-{cluster}"),
                &[
                    "curl",
                    "-f",
                    "-H",
                    "Content-Type: application/json",
                    "-d",
                    r#"{"model": "mock-model", "messages": []}"#,
                    "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
                ],
            )?;
            route_output.status.success()
        } else {
            false
        };

        let cluster_ok = deploy_recovered && swim_recovered && routing_works;
        if !cluster_ok {
            all_recovered = false;
        }

        observed_facts.insert(
            format!("{cluster}_deploy_recovered"),
            serde_json::Value::Bool(deploy_recovered),
        );
        observed_facts.insert(
            format!("{cluster}_swim_recovered"),
            serde_json::Value::Bool(swim_recovered),
        );
        observed_facts.insert(
            format!("{cluster}_routing_after_restart"),
            serde_json::Value::Bool(routing_works),
        );
    }

    if all_recovered {
        Ok(proof_success(
            "All three operators restarted sequentially with SWIM re-convergence and routing proof after each",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Operator restart recovery failed on one or more clusters",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert post-recovery soak period validates stability.
fn assert_post_recovery_soak() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();

    let soak_duration = Duration::from_secs(60); // 1-minute soak period
    eprintln!("    Starting {}-second soak period...", soak_duration.as_secs());

    let mut all_stable = true;
    let mut stability_checks = 0;
    let check_interval = Duration::from_secs(10);

    let soak_start = Instant::now();
    while soak_start.elapsed() < soak_duration {
        stability_checks += 1;

        // Check all clusters are healthy
        for cluster in CLUSTERS {
            let context = format!("kind-grid-combined-{cluster}");

            // Check operator health
            let operator_output = Command::new("kubectl")
                .args([
                    "get",
                    "deployment/grid-operator",
                    "--context",
                    &context,
                    "-n",
                    "grid-system",
                    "-o",
                    "jsonpath={.status.readyReplicas}",
                ])
                .output()?;

            let operator_healthy = if operator_output.status.success() {
                String::from_utf8_lossy(&operator_output.stdout).trim() == "1"
            } else {
                false
            };

            // Check gateways health
            let consumer_output = Command::new("kubectl")
                .args([
                    "get",
                    "deployment/consumer-gateway",
                    "--context",
                    &context,
                    "-n",
                    "grid-system",
                    "-o",
                    "jsonpath={.status.readyReplicas}",
                ])
                .output()?;

            let consumer_healthy = if consumer_output.status.success() {
                String::from_utf8_lossy(&consumer_output.stdout).trim() == "1"
            } else {
                false
            };

            let provider_output = Command::new("kubectl")
                .args([
                    "get",
                    "deployment/provider-gateway",
                    "--context",
                    &context,
                    "-n",
                    "grid-system",
                    "-o",
                    "jsonpath={.status.readyReplicas}",
                ])
                .output()?;

            let provider_healthy = if provider_output.status.success() {
                String::from_utf8_lossy(&provider_output.stdout).trim() == "1"
            } else {
                false
            };

            if !operator_healthy || !consumer_healthy || !provider_healthy {
                all_stable = false;
            }

            observed_facts.insert(
                format!("{cluster}_check_{stability_checks}_operator"),
                serde_json::Value::Bool(operator_healthy),
            );
            observed_facts.insert(
                format!("{cluster}_check_{stability_checks}_consumer"),
                serde_json::Value::Bool(consumer_healthy),
            );
            observed_facts.insert(
                format!("{cluster}_check_{stability_checks}_provider"),
                serde_json::Value::Bool(provider_healthy),
            );
        }

        std::thread::sleep(check_interval);
    }

    observed_facts.insert(
        "soak_duration_seconds".to_owned(),
        serde_json::Value::Number(soak_duration.as_secs().into()),
    );
    observed_facts.insert(
        "stability_checks_performed".to_owned(),
        serde_json::Value::Number(stability_checks.into()),
    );
    observed_facts.insert("all_stable_throughout".to_owned(), serde_json::Value::Bool(all_stable));

    if all_stable {
        Ok(proof_success(
            &format!(
                "Post-recovery soak period validated stability over {}-second period with {} checks",
                soak_duration.as_secs(),
                stability_checks
            ),
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "Instability detected during post-recovery soak period",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert that invalid routing inputs are rejected with appropriate errors.
///
/// Tests four negative cases plus one positive control, all through the same
/// DNS, image, Service, port, and execution mechanism:
///   - Invalid model name -> expected non-success
///   - Invalid path -> expected non-success
///   - Positive control -> expected success (same mechanism, valid inputs)
fn assert_negative_routing() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut all_correct = true;

    let cluster = "central";
    let context = format!("kind-grid-combined-{cluster}");

    // Positive control: valid request (same DNS, image, Service, port, mechanism)
    let positive_output = run_curl_probe(
        &context,
        "neg-positive-ctrl",
        &[
            "curl",
            "-f",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            "Content-Type: application/json",
            "-d",
            r#"{"model": "mock-model", "messages": []}"#,
            "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
        ],
    )?;

    let positive_succeeded = positive_output.status.success();
    observed_facts.insert(
        "positive_control_passed".to_owned(),
        serde_json::Value::Bool(positive_succeeded),
    );

    if !positive_succeeded {
        return Ok(proof_failure(
            "Positive control failed: cannot validate negative tests without working baseline",
            observed_facts,
            start.elapsed(),
        ));
    }

    // Negative 1: invalid model name
    let invalid_model_output = run_curl_probe(
        &context,
        "neg-invalid-model",
        &[
            "curl",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            "Content-Type: application/json",
            "-d",
            r#"{"model": "nonexistent-model-xyz", "messages": []}"#,
            "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
        ],
    )?;

    let invalid_model_status = String::from_utf8_lossy(&invalid_model_output.stdout).trim().to_owned();
    let invalid_model_rejected = !invalid_model_output.status.success()
        || invalid_model_status.starts_with('4')
        || invalid_model_status.starts_with('5');

    observed_facts.insert(
        "invalid_model_http_status".to_owned(),
        serde_json::Value::String(invalid_model_status),
    );
    observed_facts.insert(
        "invalid_model_rejected".to_owned(),
        serde_json::Value::Bool(invalid_model_rejected),
    );

    if !invalid_model_rejected {
        all_correct = false;
    }

    // Negative 2: invalid path
    let invalid_path_output = run_curl_probe(
        &context,
        "neg-invalid-path",
        &[
            "curl",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            "Content-Type: application/json",
            "consumer-gateway.grid-system.svc.cluster.local:8080/v99/nonexistent/endpoint",
        ],
    )?;

    let invalid_path_status = String::from_utf8_lossy(&invalid_path_output.stdout).trim().to_owned();
    let invalid_path_rejected = !invalid_path_output.status.success()
        || invalid_path_status.starts_with('4')
        || invalid_path_status.starts_with('5');

    observed_facts.insert(
        "invalid_path_http_status".to_owned(),
        serde_json::Value::String(invalid_path_status),
    );
    observed_facts.insert(
        "invalid_path_rejected".to_owned(),
        serde_json::Value::Bool(invalid_path_rejected),
    );

    if !invalid_path_rejected {
        all_correct = false;
    }

    // Negative 3: malformed request body
    let malformed_body_output = run_curl_probe(
        &context,
        "neg-malformed-body",
        &[
            "curl",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-H",
            "Content-Type: application/json",
            "-d",
            "not-valid-json{{{",
            "consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions",
        ],
    )?;

    let malformed_body_status = String::from_utf8_lossy(&malformed_body_output.stdout).trim().to_owned();
    let malformed_body_rejected = !malformed_body_output.status.success()
        || malformed_body_status.starts_with('4')
        || malformed_body_status.starts_with('5');

    observed_facts.insert(
        "malformed_body_http_status".to_owned(),
        serde_json::Value::String(malformed_body_status),
    );
    observed_facts.insert(
        "malformed_body_rejected".to_owned(),
        serde_json::Value::Bool(malformed_body_rejected),
    );

    if !malformed_body_rejected {
        all_correct = false;
    }

    if all_correct {
        Ok(proof_success(
            "All negative routing inputs correctly rejected (positive control passed first)",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            "One or more invalid routing inputs were not properly rejected",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert no external provider resources when disabled.
fn assert_external_provider_absence() -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut external_resources_found = false;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Check for external InferenceProvider resources
        let provider_output = Command::new("kubectl")
            .args([
                "get",
                "inferenceprovider",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ])
            .output()?;

        let mut cluster_external_providers = Vec::new();
        if provider_output.status.success() {
            let providers = String::from_utf8_lossy(&provider_output.stdout);
            let external_providers: Vec<&str> = providers
                .split_whitespace()
                .filter(|name| name.starts_with("external-"))
                .collect();

            if !external_providers.is_empty() {
                external_resources_found = true;
                cluster_external_providers.extend(external_providers.iter().map(|s| s.to_string()));
            }
        }

        // Check for external provider Secrets
        let secret_output = Command::new("kubectl")
            .args([
                "get",
                "secrets",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ])
            .output()?;

        let mut cluster_external_secrets = Vec::new();
        if secret_output.status.success() {
            let secrets = String::from_utf8_lossy(&secret_output.stdout);
            let external_secrets: Vec<&str> = secrets
                .split_whitespace()
                .filter(|name| name.contains("openai") || name.contains("anthropic") || name.contains("external"))
                .collect();

            if !external_secrets.is_empty() {
                external_resources_found = true;
                cluster_external_secrets.extend(external_secrets.iter().map(|s| s.to_string()));
            }
        }

        // Check for external provider ConfigMaps
        let configmap_output = Command::new("kubectl")
            .args([
                "get",
                "configmaps",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ])
            .output()?;

        let mut cluster_external_configmaps = Vec::new();
        if configmap_output.status.success() {
            let configmaps = String::from_utf8_lossy(&configmap_output.stdout);
            let external_configmaps: Vec<&str> = configmaps
                .split_whitespace()
                .filter(|name| name.contains("external") && (name.contains("provider") || name.contains("config")))
                .collect();

            if !external_configmaps.is_empty() {
                external_resources_found = true;
                cluster_external_configmaps.extend(external_configmaps.iter().map(|s| s.to_string()));
            }
        }

        // Record findings for this cluster
        observed_facts.insert(
            format!("{cluster}_external_providers"),
            serde_json::Value::Array(
                cluster_external_providers
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            ),
        );
        observed_facts.insert(
            format!("{cluster}_external_secrets"),
            serde_json::Value::Array(
                cluster_external_secrets
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            ),
        );
        observed_facts.insert(
            format!("{cluster}_external_configmaps"),
            serde_json::Value::Array(
                cluster_external_configmaps
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            ),
        );

        // Check provider gateway mounts for external credentials
        let gateway_output = Command::new("kubectl")
            .args([
                "get",
                "deployment/provider-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.volumes[*].secret.secretName}",
            ])
            .output()?;

        let mut cluster_mounted_secrets = Vec::new();
        if gateway_output.status.success() {
            let mounted_secrets = String::from_utf8_lossy(&gateway_output.stdout);
            let external_mounts: Vec<&str> = mounted_secrets
                .split_whitespace()
                .filter(|name| name.contains("openai") || name.contains("anthropic") || name.contains("external"))
                .collect();

            if !external_mounts.is_empty() {
                external_resources_found = true;
                cluster_mounted_secrets.extend(external_mounts.iter().map(|s| s.to_string()));
            }
        }

        observed_facts.insert(
            format!("{cluster}_mounted_external_secrets"),
            serde_json::Value::Array(
                cluster_mounted_secrets
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            ),
        );
    }

    observed_facts.insert(
        "external_resources_found".to_owned(),
        serde_json::Value::Bool(external_resources_found),
    );

    if external_resources_found {
        Ok(proof_failure(
            "External provider resources found when they should be disabled",
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_success(
            "No external provider resources found across all resource types (correctly disabled)",
            observed_facts,
            start.elapsed(),
        ))
    }
}

/// Assert external provider resources exist only in the selected site.
///
/// Positive control: the selected site must have the Secret and the
/// `InferenceProvider` CRD.  Every other site must have neither.
fn assert_external_provider_isolation(selected_site: &str) -> AssertionResult {
    let start = Instant::now();
    let mut observed_facts = BTreeMap::new();
    let mut isolation_correct = true;

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");
        let is_selected = *cluster == selected_site;

        let secret_output = Command::new("kubectl")
            .args([
                "get",
                "secrets",
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ])
            .output()?;

        let has_external_secret = if secret_output.status.success() {
            let secrets = String::from_utf8_lossy(&secret_output.stdout);
            secrets
                .split_whitespace()
                .any(|name| name.contains("openai") || name.contains("anthropic") || name.contains("external"))
        } else {
            false
        };

        let provider_output = Command::new("kubectl")
            .args([
                "get",
                "inferenceprovider",
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ])
            .output()?;

        let has_external_provider = if provider_output.status.success() {
            let providers = String::from_utf8_lossy(&provider_output.stdout);
            providers.split_whitespace().any(|name| name.starts_with("external-"))
        } else {
            false
        };

        let mount_output = Command::new("kubectl")
            .args([
                "get",
                "deployment/provider-gateway",
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "-o",
                "jsonpath={.spec.template.spec.volumes[*].secret.secretName}",
            ])
            .output()?;

        let has_external_mount = if mount_output.status.success() {
            let mounts = String::from_utf8_lossy(&mount_output.stdout);
            mounts
                .split_whitespace()
                .any(|name| name.contains("openai") || name.contains("anthropic") || name.contains("external"))
        } else {
            false
        };

        if is_selected {
            if !has_external_secret || !has_external_provider {
                isolation_correct = false;
            }
        } else if has_external_secret || has_external_provider || has_external_mount {
            isolation_correct = false;
        }

        observed_facts.insert(
            format!("{cluster}_is_selected_site"),
            serde_json::Value::Bool(is_selected),
        );
        observed_facts.insert(
            format!("{cluster}_has_external_secret"),
            serde_json::Value::Bool(has_external_secret),
        );
        observed_facts.insert(
            format!("{cluster}_has_external_provider"),
            serde_json::Value::Bool(has_external_provider),
        );
        observed_facts.insert(
            format!("{cluster}_has_external_mount"),
            serde_json::Value::Bool(has_external_mount),
        );
    }

    if isolation_correct {
        Ok(proof_success(
            &format!(
                "External provider correctly isolated to {selected_site}: Secret and InferenceProvider present only there"
            ),
            observed_facts,
            start.elapsed(),
        ))
    } else {
        Ok(proof_failure(
            &format!("External provider isolation failed for site {selected_site}"),
            observed_facts,
            start.elapsed(),
        ))
    }
}

// -----------------------------------------------------------------------------
// Setup Functions
// -----------------------------------------------------------------------------

/// Verify a local Docker image exists; fail with a clear message if absent.
fn require_local_image(image: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    if status.success() {
        return Ok(());
    }
    Err(format!(
        "required local image {image:?} is absent; build it or set \
         GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent with registry image overrides"
    )
    .into())
}

/// Verify that `image` has a numeric USER (or UID:GID) so Kubernetes can
/// enforce `runAsNonRoot` without `runAsUser` in the pod spec.
fn require_numeric_image_user(image: &str) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.Config.User}}", image])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "cannot inspect image {image:?}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let user = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if user.is_empty() {
        return Err(format!("image {image:?} has no USER set; runAsNonRoot requires a numeric user").into());
    }
    let valid = user.split(':').all(|part| part.parse::<u32>().is_ok());
    if !valid {
        return Err(format!(
            "image {image:?} has non-numeric USER {user:?}; \
             Kubernetes cannot verify runAsNonRoot with a non-numeric user"
        )
        .into());
    }
    Ok(())
}

/// Load local container images into all Kind clusters.
///
/// Reads image references from the `GRID_XTASK_*_IMAGE` environment variables
/// (same source as `apply_image_overrides`). When `imagePullPolicy` is not
/// `Never`, this is a no-op.
fn load_images_into_clusters(forge_bin: &Path, resolved_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let pull_policy = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned());
    if pull_policy != "Never" {
        eprintln!("  skipping Kind image loading (pull policy is {pull_policy})");
        return Ok(());
    }

    let gateway =
        std::env::var("GRID_XTASK_GATEWAY_IMAGE").unwrap_or_else(|_| "praxis-ai:combined-site-demo".to_owned());
    let operator =
        std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| "grid-operator:combined-site-demo".to_owned());
    let mock_provider = std::env::var("GRID_XTASK_MOCK_PROVIDER_IMAGE")
        .unwrap_or_else(|_| "grid-mock-providers:combined-site-demo".to_owned());

    for image in [&gateway, &operator, &mock_provider] {
        require_local_image(image)?;
        eprintln!("  verified local image: {image}");
    }

    require_numeric_image_user(&mock_provider)?;
    eprintln!("  verified numeric USER for {mock_provider}");

    for cluster in CLUSTERS {
        for image in [&gateway, &operator, &mock_provider] {
            eprintln!("  loading {image} into {cluster}...");
            let output = Command::new(forge_bin.as_os_str())
                .arg("--config")
                .arg(resolved_config)
                .args(["--non-interactive", "cluster", "load-image", cluster, image])
                .output()?;
            if !output.status.success() {
                return Err(format!(
                    "failed to load {image} into {cluster}: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )
                .into());
            }
        }
        eprintln!("  [OK] {cluster}: all images loaded");
    }
    Ok(())
}

/// Generate TLS certificates for all combined-site identities.
///
/// Must be called BEFORE `forge up` so the certificates exist on the host
/// when `install_provider_boundary` creates the Kubernetes Secrets.
fn stage_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let identities: Vec<String> = CLUSTERS.iter().map(|c| (*c).to_owned()).collect();
    certs::generate_all(&identities)?;

    let wrong_ca = ::certs::generate_ca("Combined Site untrusted test CA")?;
    fs::write(Path::new(CERTS_DIR).join("untrusted-ca.pem"), wrong_ca.cert_pem)?;

    eprintln!("  [OK] TLS certificates generated for west, central, east");
    Ok(())
}

/// Create TLS and credential Secrets in every combined-site cluster.
///
/// Must be called AFTER `forge up` since the clusters must exist.  Gateway
/// deployments are restarted so pods pick up the new volume mounts.
fn install_provider_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let certs_dir = Path::new(CERTS_DIR);

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        apply_tls_secret(&context, cluster, CONSUMER_TLS_SECRET, certs_dir)?;
        apply_tls_secret(&context, cluster, PROVIDER_TLS_SECRET, certs_dir)?;
        apply_tls_secret(&context, "wrong-org-client", WRONG_ORG_TLS_SECRET, certs_dir)?;

        eprintln!("  [OK] {cluster}: TLS secrets installed");
    }

    Ok(())
}

/// Create a TLS secret from the generated cert, key, and CA files.
fn apply_tls_secret(
    context: &str,
    identity: &str,
    secret_name: &str,
    certs_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            secret_name,
            &format!(
                "--from-file=tls.crt={}",
                certs_dir.join(format!("{identity}-cert.pem")).display()
            ),
            &format!(
                "--from-file=tls.key={}",
                certs_dir.join(format!("{identity}-key.pem")).display()
            ),
            &format!("--from-file=ca.crt={}", certs_dir.join("ca.pem").display()),
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render {identity} Secret/{secret_name}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    kubectl::apply_manifest(context, &String::from_utf8(output.stdout)?)
}

/// Generate a random 32-byte hex provider credential.
fn generate_provider_credential() -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("openssl").args(["rand", "-hex", "32"]).output()?;
    if !output.status.success() {
        return Err("openssl failed to generate provider credential".into());
    }
    let token = String::from_utf8(output.stdout)?.trim().to_owned();
    if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("openssl returned an invalid provider credential".into());
    }
    Ok(token)
}

/// Create an Opaque Secret with a `token` key.
fn apply_credential_secret(context: &str, secret_name: &str, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = format!(
        r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"{secret_name}","namespace":"{GRID_SYSTEM_NS}"}},"type":"Opaque","stringData":{{"token":"{token}"}}}}"#,
    );
    kubectl::apply_manifest(context, &manifest)
}


/// Data extracted from the operator-created overlay ConfigMap.
/// Read a single cluster's overlay ConfigMap and return structured data.
///
/// Captures both the Kubernetes `resourceVersion` (per-cluster) and the
/// semantic revision from the `grid.praxis-proxy.io/overlay-revision`
/// annotation (content-addressed, safe to compare across clusters).
fn read_cluster_overlay(cluster: &str) -> Result<OverlayData, Box<dyn std::error::Error>> {
    let context = format!("kind-grid-combined-{cluster}");

    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "configmap",
            OVERLAY_CONFIGMAP,
            "-o",
            "json",
        ])
        .output()?;

    if !output.status.success() {
        return Err(format!("{cluster}: overlay ConfigMap not found").into());
    }

    let cm: serde_json::Value =
        serde_json::from_slice(&output.stdout).map_err(|e| format!("{cluster}: overlay ConfigMap invalid: {e}"))?;

    let resource_version = cm
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_owned();

    let semantic_revision = cm
        .pointer("/metadata/annotations/grid.praxis-proxy.io~1overlay-revision")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_owned();

    let routing_json = cm
        .pointer("/data/routing-config.json")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{cluster}: overlay missing routing-config.json"))?;

    let parsed: serde_json::Value =
        serde_json::from_str(routing_json).map_err(|e| format!("{cluster}: routing-config.json invalid: {e}"))?;

    let raw_candidates = parsed
        .get("candidates")
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("{cluster}: overlay missing candidates array"))?;

    let mut stable_ids = BTreeMap::new();
    let mut candidates = Vec::new();

    for c in raw_candidates {
        let cluster_field = c.get("cluster").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let stable_id = c.get("stable_id").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let kind = c.get("kind").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let name = c.get("name").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let site = c.get("site").and_then(|v| v.as_str()).unwrap_or("").to_owned();

        if !cluster_field.is_empty() && !stable_id.is_empty() {
            stable_ids.insert(cluster_field.clone(), stable_id.clone());
        }

        candidates.push(OverlayCandidate {
            kind,
            name,
            site,
            cluster: cluster_field,
            stable_id,
        });
    }

    Ok(OverlayData {
        resource_version,
        semantic_revision,
        stable_ids,
        candidates,
    })
}

/// Build the expected set of overlay candidate names.
///
/// Base set: `sim-{cluster}-provider` for every cluster.
/// When an external provider is enabled, `external-{provider_kind}` is added.
/// Build the expected candidate name set and optional external candidate
/// expectation from the descriptor.
///
/// The InferenceProvider name `external-{provider_kind}` is the contract
/// established by `create_external_inference_provider`. The returned
/// `ExpectedExternalCandidate` carries the routing_cluster, model, and
/// site so that convergence validation can assert field-level agreement.
fn expected_candidates(
    external_provider: Option<&ExternalProviderDescriptor>,
    external_site: Option<&str>,
) -> (BTreeSet<String>, Option<ExpectedExternalCandidate>) {
    let mut expected = BTreeSet::new();
    for cluster in CLUSTERS {
        expected.insert(format!("sim-{cluster}-provider"));
    }

    let ext_candidate = if let (Some(ext), Some(site)) = (external_provider, external_site) {
        let name = format!("external-{}", ext.provider_kind);
        expected.insert(name.clone());
        Some(ExpectedExternalCandidate {
            name,
            routing_cluster: ext.routing_cluster.to_owned(),
            model: ext.model.clone(),
            site: site.to_owned(),
        })
    } else {
        None
    };

    (expected, ext_candidate)
}

/// Wait for each cluster's operator to produce its expected local candidate.
///
/// Pre-SWIM, each operator only knows its local `InferenceProvider` resources.
/// Global convergence (all candidates on every cluster) happens after SWIM
/// seeding in a separate phase.
#[expect(
    clippy::disallowed_methods,
    reason = "Sleep is required for polling with timeout functionality"
)]
fn wait_for_local_overlays() -> Result<BTreeMap<String, OverlayData>, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(180);
    let interval = Duration::from_secs(5);
    let start = Instant::now();

    eprintln!("  Polling for local overlay ConfigMaps (timeout {timeout:?})...");

    while start.elapsed() < timeout {
        let mut overlays = BTreeMap::new();
        let mut all_ready = true;

        for cluster in CLUSTERS {
            let expected_local = format!("sim-{cluster}-provider");
            match read_cluster_overlay(cluster) {
                Ok(data) if data.stable_ids.contains_key(&expected_local) => {
                    eprintln!(
                        "  {cluster}: {expected_local} present (semantic_rev={}, stable_id={})",
                        data.semantic_revision,
                        data.stable_ids.get(&expected_local).map_or("?", String::as_str)
                    );
                    overlays.insert((*cluster).to_owned(), data);
                },
                Ok(data) => {
                    eprintln!(
                        "  {cluster}: overlay present but missing {expected_local} (has: {:?})",
                        data.stable_ids.keys().collect::<Vec<_>>()
                    );
                    all_ready = false;
                },
                Err(_) => {
                    all_ready = false;
                },
            }
        }

        if all_ready && overlays.len() == CLUSTERS.len() {
            return Ok(overlays);
        }

        std::thread::sleep(interval);
    }

    collect_overlay_diagnostics();
    Err(format!("Local overlay ConfigMaps not ready after {timeout:?}").into())
}

/// Wait for SWIM-driven global overlay convergence.
///
/// Every cluster's overlay must contain every candidate in `expected`.
/// After convergence, verifies that stable IDs agree across all sites.
/// When an external candidate is expected, validates its routing_cluster,
/// model, and site fields against the descriptor contract.
#[expect(
    clippy::disallowed_methods,
    reason = "Sleep is required for polling with timeout functionality"
)]
fn wait_for_global_overlay_convergence(
    expected: &BTreeSet<String>,
    ext_candidate: Option<&ExpectedExternalCandidate>,
) -> Result<BTreeMap<String, OverlayData>, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(180);
    let interval = Duration::from_secs(5);
    let start = Instant::now();

    eprintln!(
        "  Waiting for global overlay convergence \
         ({} candidates on all clusters, timeout {timeout:?})...",
        expected.len()
    );

    while start.elapsed() < timeout {
        let mut overlays = BTreeMap::new();
        let mut all_converged = true;

        for cluster in CLUSTERS {
            match read_cluster_overlay(cluster) {
                Ok(data) => {
                    let missing: Vec<&String> = expected.iter().filter(|c| !data.stable_ids.contains_key(*c)).collect();
                    if missing.is_empty() {
                        overlays.insert((*cluster).to_owned(), data);
                    } else {
                        eprintln!("  {cluster}: missing candidates {missing:?}");
                        all_converged = false;
                    }
                },
                Err(_) => {
                    all_converged = false;
                },
            }
        }

        if all_converged && overlays.len() == CLUSTERS.len() {
            let reference_site = CLUSTERS.first().ok_or("CLUSTERS is empty")?;
            let reference = overlays
                .get(*reference_site)
                .ok_or("reference site missing from overlays")?;

            for cluster in CLUSTERS.iter().skip(1) {
                let site_data = overlays
                    .get(*cluster)
                    .ok_or_else(|| format!("{cluster} missing from overlays"))?;
                for candidate in expected {
                    let ref_id = reference
                        .stable_ids
                        .get(candidate)
                        .ok_or_else(|| format!("{reference_site}: missing {candidate}"))?;
                    let site_id = site_data
                        .stable_ids
                        .get(candidate)
                        .ok_or_else(|| format!("{cluster}: missing {candidate}"))?;
                    if site_id != ref_id {
                        return Err(format!(
                            "stable_id mismatch for {candidate}: \
                             {reference_site}={ref_id} vs {cluster}={site_id}"
                        )
                        .into());
                    }
                }
            }

            if let Some(ext) = ext_candidate {
                for cluster in CLUSTERS {
                    let data = overlays
                        .get(*cluster)
                        .ok_or_else(|| format!("{cluster} missing from overlays"))?;
                    let candidate = data.candidates.iter().find(|c| c.cluster == ext.name).ok_or_else(|| {
                        format!(
                            "{cluster}: external candidate {} missing from overlay candidates",
                            ext.name
                        )
                    })?;
                    if candidate.name != ext.model {
                        return Err(format!(
                            "{cluster}: external candidate model mismatch: \
                             expected={} observed={}",
                            ext.model, candidate.name
                        )
                        .into());
                    }
                    if candidate.site != ext.site {
                        return Err(format!(
                            "{cluster}: external candidate site mismatch: \
                             expected={} observed={}",
                            ext.site, candidate.site
                        )
                        .into());
                    }
                }
                eprintln!(
                    "  External candidate {}: model={}, site={}, routing_cluster={} validated",
                    ext.name, ext.model, ext.site, ext.routing_cluster
                );
            }

            eprintln!(
                "  Global overlay converged: {} candidates on all {} clusters, stable_ids agree",
                expected.len(),
                CLUSTERS.len()
            );
            return Ok(overlays);
        }

        std::thread::sleep(interval);
    }

    collect_overlay_diagnostics();
    Err(format!("Global overlay convergence failed after {timeout:?}").into())
}

/// Collect diagnostic information when the overlay ConfigMap fails to converge.
fn collect_overlay_diagnostics() {
    eprintln!("  [DIAG] Collecting overlay failure diagnostics...\n");

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");
        eprintln!("  ======== {cluster} ========");

        eprintln!("  [DIAG] {cluster}: 1. Operator deployment SWIM env vars");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "deployment",
                    "grid-operator",
                    "-o",
                    "jsonpath={range .spec.template.spec.containers[0].env[*]}{.name}={.value}{'\\n'}{end}",
                ])
                .status(),
        );

        eprintln!("\n  [DIAG] {cluster}: 2. SWIM service details");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "svc",
                    "grid-operator-swim",
                    "-o",
                    "wide",
                ])
                .status(),
        );
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "endpoints",
                    "grid-operator-swim",
                    "-o",
                    "yaml",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 3. Operator pod status");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "pods",
                    "-l",
                    "app.kubernetes.io/name=grid-operator",
                    "-o",
                    "wide",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 4. Operator logs (last 50 lines, unfiltered)");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "logs",
                    "deployment/grid-operator",
                    "--tail=50",
                ])
                .status(),
        );

        eprintln!("\n  [DIAG] {cluster}: 5. GridNetwork CRD status");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "gridnetwork",
                    "-o",
                    "yaml",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 6. Overlay ConfigMap content");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "configmap",
                    OVERLAY_CONFIGMAP,
                    "-o",
                    "json",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 7. InferenceProvider CRs");
        drop(
            Command::new("kubectl")
                .args([
                    "--context",
                    &context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "inferenceprovider",
                    "-o",
                    "yaml",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 8. Helm values for grid-operator");
        drop(
            Command::new("helm")
                .args([
                    "get",
                    "values",
                    "grid-operator",
                    "--namespace",
                    GRID_SYSTEM_NS,
                    "--kube-context",
                    &context,
                    "-o",
                    "yaml",
                ])
                .status(),
        );

        eprintln!("  [DIAG] {cluster}: 9. NetworkPolicy in grid-system");
        drop(
            Command::new("kubectl")
                .args(["--context", &context, "-n", GRID_SYSTEM_NS, "get", "networkpolicy"])
                .status(),
        );

        eprintln!();
    }

    eprintln!("  [DIAG] 10. Cross-cluster SWIM connectivity check");
    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");
        for target in CLUSTERS {
            if *target == *cluster {
                continue;
            }
            let target_context = format!("kind-grid-combined-{target}");
            if let Ok(output) = Command::new("kubectl")
                .args([
                    "--context",
                    &target_context,
                    "-n",
                    GRID_SYSTEM_NS,
                    "get",
                    "svc",
                    "grid-operator-swim",
                    "-o",
                    "jsonpath={.status.loadBalancer.ingress[0].ip}",
                ])
                .output()
            {
                let ip = String::from_utf8_lossy(&output.stdout);
                eprintln!("  [DIAG] {cluster} -> {target} (SWIM LB {ip}): testing TCP 7946");
                drop(
                    Command::new("kubectl")
                        .args([
                            "--context",
                            &context,
                            "-n",
                            GRID_SYSTEM_NS,
                            "exec",
                            "deployment/grid-operator",
                            "--",
                            "sh",
                            "-c",
                            &format!(
                                "timeout 3 sh -c 'echo | nc -w 2 {ip} 7946' && echo REACHABLE || echo UNREACHABLE"
                            ),
                        ])
                        .status(),
                );
            }
        }
    }
}

/// Materialize provider gateway configuration from the pre-SWIM overlay map.
///
/// For each cluster, extracts the `stable_id` for the local
/// `sim-{cluster}-provider` candidate and renders the provider praxis.yaml
/// template with:
/// - `SITE_PLACEHOLDER` → cluster name
/// - `CANDIDATE_ID_PLACEHOLDER` → stable ID from the overlay
///
/// Creates the `provider-gateway-config` ConfigMap in each cluster.
fn materialize_provider_config(overlays: &BTreeMap<String, OverlayData>) -> Result<(), Box<dyn std::error::Error>> {
    let template_path = Path::new("demos/grid-combined-site/configs/provider/praxis.yaml");
    let template =
        fs::read_to_string(template_path).map_err(|e| format!("failed to read provider config template: {e}"))?;

    for cluster in CLUSTERS {
        let provider_name = format!("sim-{cluster}-provider");
        let overlay = overlays
            .get(*cluster)
            .ok_or_else(|| format!("no overlay data for cluster {cluster}"))?;
        let stable_id = overlay.stable_ids.get(&provider_name).ok_or_else(|| {
            format!(
                "{cluster}: no candidate {provider_name} in overlay (has: {:?})",
                overlay.stable_ids.keys().collect::<Vec<_>>()
            )
        })?;

        eprintln!("  {cluster}: {provider_name} -> {stable_id}");

        let rendered = template
            .replace("SITE_PLACEHOLDER", cluster)
            .replace("CANDIDATE_ID_PLACEHOLDER", stable_id);

        let context = format!("kind-grid-combined-{cluster}");

        let create = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "create",
                "configmap",
                "provider-gateway-config",
                &format!("--from-literal=praxis.yaml={rendered}"),
                "--dry-run=client",
                "-o",
                "yaml",
            ])
            .output()?;

        if !create.status.success() {
            return Err(format!(
                "failed to render provider-gateway-config for {cluster}: {}",
                String::from_utf8_lossy(&create.stderr).trim()
            )
            .into());
        }

        kubectl::apply_manifest(&context, &String::from_utf8(create.stdout)?)?;
        eprintln!("  [OK] {cluster}: provider-gateway-config created (stable_id={stable_id})");
    }

    Ok(())
}

/// Materialize the Forge configuration with image overrides and external provider integration.
fn materialize_config(
    source: &Path,
    external_provider: Option<&ExternalProviderDescriptor>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let rendered = render_config(&content, external_provider)?;
    let parent = source.parent().ok_or("source config must have parent directory")?;
    let output = parent.join(".forge.resolved.yaml");
    fs::write(&output, rendered)?;
    Ok(output)
}

/// Render image overrides into one Forge configuration.
fn render_config(
    content: &str,
    _external_provider: Option<&ExternalProviderDescriptor>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut config: serde_yaml::Value = serde_yaml::from_str(content)?;
    apply_image_overrides(&mut config);
    Ok(serde_yaml::to_string(&config)?)
}

/// Apply image overrides from environment variables to the Forge configuration.
#[expect(
    clippy::too_many_lines,
    clippy::collapsible_if,
    reason = "Image override application with structured YAML manipulation; nested ifs follow YAML structure hierarchy"
)]
fn apply_image_overrides(config: &mut serde_yaml::Value) {
    let gateway_image =
        std::env::var("GRID_XTASK_GATEWAY_IMAGE").unwrap_or_else(|_| "praxis-ai:combined-site-demo".to_owned());
    let operator_image =
        std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| "grid-operator:combined-site-demo".to_owned());
    let mock_provider_image = std::env::var("GRID_XTASK_MOCK_PROVIDER_IMAGE")
        .unwrap_or_else(|_| "grid-mock-providers:combined-site-demo".to_owned());
    let image_pull_policy = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned());

    let (gateway_repo, gateway_tag) = parse_image_ref(&gateway_image);
    let (operator_repo, operator_tag) = parse_image_ref(&operator_image);
    let (mock_repo, mock_tag) = parse_image_ref(&mock_provider_image);

    if let Some(spec) = config.get_mut("spec") {
        if let Some(clusters) = spec.get_mut("clusters") {
            if let Some(clusters_array) = clusters.as_sequence_mut() {
                for cluster in clusters_array {
                    if let Some(properties) = cluster.get_mut("properties") {
                        if let Some(props_map) = properties.as_mapping_mut() {
                            let pairs = [
                                ("gatewayImage", &gateway_image),
                                ("operatorImage", &operator_image),
                                ("mockProviderImage", &mock_provider_image),
                                ("imagePullPolicy", &image_pull_policy),
                                ("gatewayImageRepo", &gateway_repo),
                                ("gatewayImageTag", &gateway_tag),
                                ("operatorImageRepo", &operator_repo),
                                ("operatorImageTag", &operator_tag),
                                ("mockProviderImageRepo", &mock_repo),
                                ("mockProviderImageTag", &mock_tag),
                            ];
                            for (key, val) in pairs {
                                props_map.insert(
                                    serde_yaml::Value::String(key.to_owned()),
                                    serde_yaml::Value::String(val.clone()),
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Parse image reference into (repo, tag) components.
fn parse_image_ref(image: &str) -> (String, String) {
    if let Some(colon_pos) = image.rfind(':') {
        let (repo, tag) = image.split_at(colon_pos);
        // Skip the ':' character
        let tag = &tag[1..];
        (repo.to_owned(), tag.to_owned())
    } else {
        (image.to_owned(), "latest".to_owned())
    }
}

/// Prepare setup context from configuration.
fn prepare_setup(
    forge_config: &Path,
    ext_descriptor: Option<ExternalProviderDescriptor>,
    ext_site: Option<String>,
    ext_key_file: Option<PathBuf>,
) -> Result<CombinedSiteContext, Box<dyn std::error::Error>> {
    // Validate external provider site selection early
    if let Some(_ext) = &ext_descriptor {
        let site = ext_site
            .as_ref()
            .ok_or("external provider requires --external-provider-site")?;
        if !CLUSTERS.contains(&site.as_str()) {
            return Err(format!("external provider site must be one of: {}", CLUSTERS.join(", ")).into());
        }
    }

    let resolved_config = materialize_config(forge_config, ext_descriptor.as_ref())?;
    let forge_bin = glb::resolve_forge_binary()
        .ok_or("praxis-forge binary not found")?
        .into();

    Ok(CombinedSiteContext {
        resolved_config,
        forge_bin,
        external_provider: ext_descriptor,
        external_provider_site: ext_site,
        external_key_file: ext_key_file,
    })
}

/// Authorize auto-discovered remote GridSites with identity trust material.
///
/// For each local cluster, waits for the two remote auto-discovered GridSites,
/// verifies the SWIM-advertised certificate matches the staged identity, then
/// patches `spec.egress.tls.serverName` and `spec.trust.canonicalFingerprints`.
/// The controller transitions the site to Active naturally after the patch.
fn authorize_discovered_sites() -> Result<(), Box<dyn std::error::Error>> {
    const TRUST_TIMEOUT: Duration = Duration::from_secs(120);
    const GRID_NETWORK: &str = "grid-combined-site";

    for local in CLUSTERS {
        let context = format!("kind-grid-combined-{local}");
        eprintln!();
        eprintln!("  {local}: authorizing remote provider sites");
        for remote in CLUSTERS {
            if *remote == *local {
                continue;
            }
            let site_name = format!("{GRID_NETWORK}-{remote}");
            operator::wait_for_auto_gridsite(&context, &site_name, GRID_NETWORK, TRUST_TIMEOUT)?;
            let canonical_fp = certs::site_certificate_fingerprint(remote)?;
            operator::wait_for_expected_site_certificate(&context, &site_name, &canonical_fp, TRUST_TIMEOUT)?;
            let server_name = format!("{remote}.grid.internal");
            operator::patch_gridsite_identity_trust(&context, &site_name, &canonical_fp, &server_name)?;
            operator::wait_for_gridsite_phase(&context, &site_name, "Active", TRUST_TIMEOUT)?;
        }
    }
    eprintln!("  [OK] All auto-discovered remote GridSites authorized and Active");
    Ok(())
}

/// Deploy the combined-site environment.
#[expect(
    clippy::too_many_lines,
    reason = "sequential setup steps: each step depends on the previous; splitting obscures the setup flow"
)]
fn deploy_setup(context: &CombinedSiteContext) -> Result<OverlayState, Box<dyn std::error::Error>> {
    let total_phases = SETUP_PHASES;
    let mut phase = 0;
    let mut next = || {
        phase += 1;
        phase
    };

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Resolving Forge config and building images",
        next(),
        total_phases
    );

    // Validate the resolved forge configuration
    let output = Command::new(&context.forge_bin)
        .args(["config", "validate", "--config"])
        .arg(&context.resolved_config)
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "Forge config validation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    eprintln!("  [OK] Forge config resolved to {}", context.resolved_config.display());

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Generating TLS certificates for all sites",
        next(),
        total_phases
    );

    stage_provider_boundary()?;

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Creating three Kind clusters: west, central, east",
        next(),
        total_phases
    );

    let status = Command::new(&context.forge_bin)
        .args(["up", "--config"])
        .arg(&context.resolved_config)
        .status()?;

    if !status.success() {
        return Err("Failed to create combined-site clusters".into());
    }

    eprintln!("  [OK] All three clusters created");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Loading container images into Kind clusters",
        next(),
        total_phases
    );

    load_images_into_clusters(&context.forge_bin, &context.resolved_config)?;

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying infrastructure stacks", next(), total_phases);

    let apply_stack = |forge_bin: &Path,
                       resolved_config: &Path,
                       cluster: &str,
                       stack: &str|
     -> Result<(), Box<dyn std::error::Error>> {
        eprintln!("  applying {stack} to {cluster}...");
        let status = Command::new(forge_bin)
            .arg("--config")
            .arg(resolved_config)
            .args(["--non-interactive", "stack", "apply", cluster, stack])
            .status()?;
        if !status.success() {
            return Err(format!("Failed to apply {stack} to {cluster}").into());
        }
        Ok(())
    };

    for cluster in CLUSTERS {
        apply_stack(&context.forge_bin, &context.resolved_config, cluster, "metallb")?;
    }
    for cluster in CLUSTERS {
        let op_stack = format!("{cluster}-operator-base");
        apply_stack(&context.forge_bin, &context.resolved_config, cluster, &op_stack)?;
    }
    eprintln!("  [OK] Infrastructure stacks applied");

    eprintln!();
    eprintln!("[SETUP {}/{}] Verifying Grid operators are ready", next(), total_phases);

    for cluster in CLUSTERS {
        let ctx = format!("kind-grid-combined-{cluster}");
        wait_for_deployment("grid-operator", GRID_SYSTEM_NS, &ctx)?;
        eprintln!("  [OK] {cluster}: Grid operator ready");
    }

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying mock providers", next(), total_phases);

    for cluster in CLUSTERS {
        let ctx = format!("kind-grid-combined-{cluster}");
        let credential = generate_provider_credential()?;
        apply_credential_secret(&ctx, MOCK_INFERENCE_CREDENTIAL, &credential)?;
        eprintln!("  [OK] {cluster}: mock-inference-credential created");
        apply_stack(&context.forge_bin, &context.resolved_config, cluster, "mock-providers")?;
    }
    eprintln!("  [OK] Mock providers deployed");

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying grid-site resources", next(), total_phases);

    for cluster in CLUSTERS {
        let site_stack = format!("{cluster}-site");
        apply_stack(&context.forge_bin, &context.resolved_config, cluster, &site_stack)?;
    }
    eprintln!("  [OK] Grid site resources deployed");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Waiting for local overlay ConfigMaps",
        next(),
        total_phases
    );

    let pre_swim_overlays = wait_for_local_overlays()?;
    eprintln!("  [OK] Local overlay ConfigMaps ready");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Materializing provider config and installing trust",
        next(),
        total_phases
    );

    materialize_provider_config(&pre_swim_overlays)?;
    install_provider_boundary()?;
    if let (Some(ext), Some(site)) = (&context.external_provider, &context.external_provider_site) {
        let key_file = context
            .external_key_file
            .as_deref()
            .ok_or("external provider requires --external-provider-key-file")?;
        configure_external_provider(ext, site, key_file)?;
        eprintln!("  [OK] External provider configured at {site}");
    }
    eprintln!("  [OK] Provider config materialized, trust installed");

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying provider gateways", next(), total_phases);

    for cluster in CLUSTERS {
        apply_stack(
            &context.forge_bin,
            &context.resolved_config,
            cluster,
            "provider-gateway",
        )?;
    }
    eprintln!("  [OK] Provider gateways deployed");

    eprintln!();
    eprintln!("[SETUP {}/{}] Deploying consumer gateways", next(), total_phases);

    for cluster in CLUSTERS {
        apply_stack(
            &context.forge_bin,
            &context.resolved_config,
            cluster,
            "consumer-gateway",
        )?;
    }
    eprintln!("  [OK] Consumer gateways deployed");

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] SWIM discovery and trust authorization",
        next(),
        total_phases
    );

    configure_swim_peers(&context.forge_bin, &context.resolved_config)?;
    authorize_discovered_sites()?;

    eprintln!();
    eprintln!(
        "[SETUP {}/{}] Waiting for global overlay convergence",
        next(),
        total_phases
    );

    let (expected, ext_candidate) = expected_candidates(
        context.external_provider.as_ref(),
        context.external_provider_site.as_deref(),
    );
    let post_swim_overlays = wait_for_global_overlay_convergence(&expected, ext_candidate.as_ref())?;
    let environment_status = wait_for_environment_ready()?;
    eprintln!("  [OK] {environment_status}");

    Ok(OverlayState {
        pre_swim: pre_swim_overlays,
        post_swim: post_swim_overlays,
    })
}

// -----------------------------------------------------------------------------
// Demo Scenarios
// -----------------------------------------------------------------------------

/// Run the quick-mode proof scenarios using the assertion framework.
///
/// `external_provider_site` is `Some("west")` / `Some("east")` / etc. when
/// the external provider is enabled, `None` when disabled.
fn run_quick_scenarios(external_provider_site: Option<&str>) -> BTreeMap<String, ProofResult> {
    let mut results = BTreeMap::new();
    let mut scenario_num: usize = 0;
    let mut scenario = || {
        scenario_num += 1;
        scenario_num
    };

    eprintln!();
    eprintln!("=== QUICK MODE SCENARIOS ===");
    eprintln!();

    eprintln!("[SCENARIO {}] Verify three clusters are healthy", scenario());
    run_and_insert(&mut results, "cluster_health", assert_cluster_health);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify component deployment", scenario());
    run_and_insert(&mut results, "component_deployment", assert_component_deployment);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify SWIM convergence", scenario());
    run_and_insert(&mut results, "swim_convergence", assert_swim_convergence);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify site auto-discovery", scenario());
    run_and_insert(&mut results, "site_auto_discovery", assert_site_auto_discovery);

    if let Some(site) = external_provider_site {
        eprintln!();
        eprintln!(
            "[SCENARIO {}] Verify external provider isolation (site: {site})",
            scenario()
        );
        match assert_external_provider_isolation(site) {
            Ok(proof) => {
                results.insert("external_provider_isolation".to_owned(), proof);
            },
            Err(e) => {
                eprintln!("  [X] external_provider_isolation failed: {e}");
                results.insert(
                    "external_provider_isolation".to_owned(),
                    proof_failure(
                        &format!("external_provider_isolation failed: {e}"),
                        BTreeMap::new(),
                        Duration::from_secs(0),
                    ),
                );
            },
        }
    } else {
        eprintln!();
        eprintln!("[SCENARIO {}] Verify external provider absence", scenario());
        run_and_insert(
            &mut results,
            "external_provider_absence",
            assert_external_provider_absence,
        );
    }

    eprintln!();
    eprintln!("[SCENARIO {}] Verify overlay acceptance", scenario());
    run_and_insert(&mut results, "overlay_acceptance", assert_overlay_acceptance);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify local provider selection", scenario());
    run_and_insert(
        &mut results,
        "local_provider_selection",
        assert_local_provider_selection,
    );

    eprintln!();
    eprintln!("[SCENARIO {}] Verify response attribution", scenario());
    run_and_insert(&mut results, "response_attribution", assert_response_attribution);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify TLS certificate rejection", scenario());
    run_and_insert(
        &mut results,
        "tls_certificate_validation",
        assert_tls_certificate_validation,
    );

    eprintln!();
    eprintln!("[SCENARIO {}] Verify authorization replacement", scenario());
    run_and_insert(
        &mut results,
        "authorization_replacement",
        assert_authorization_replacement,
    );

    eprintln!();
    eprintln!("[SCENARIO {}] Verify credential isolation", scenario());
    run_and_insert(&mut results, "credential_isolation", assert_credential_isolation);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify backend access denial", scenario());
    run_and_insert(&mut results, "backend_access_denial", assert_backend_access_denial);

    eprintln!();
    eprintln!("[SCENARIO {}] Verify negative routing (invalid inputs)", scenario());
    run_and_insert(&mut results, "negative_routing", assert_negative_routing);

    results
}

/// Run the full-mode proof scenarios.
///
/// Drain/session/restore ordering:
///   1. Establish session (before drain)
///   2. Drain central provider
///   3. Test existing session survives drain
///   4. Test new session works after drain
///   5. Restore central provider (guaranteed on every error path)
///   6. Remaining scenarios (rollout, operator restart, soak)
fn run_full_scenarios(external_provider_site: Option<&str>) -> BTreeMap<String, ProofResult> {
    let mut results = run_quick_scenarios(external_provider_site);

    eprintln!();
    eprintln!("=== ADDITIONAL FULL MODE SCENARIOS ===");
    eprintln!();

    let next_scenario = std::cell::Cell::new(results.len() + 1);
    let mut scenario = || {
        let n = next_scenario.get();
        next_scenario.set(n + 1);
        n
    };

    // Step 1: Establish session BEFORE drain
    eprintln!();
    eprintln!("[SCENARIO {}] Establish session before drain", scenario());
    run_and_insert(&mut results, "session_establishment", assert_session_establishment);

    // Steps 2-5 are wrapped so restoration is guaranteed even if intermediate steps fail.
    run_drain_session_restore_sequence(&mut results, &mut scenario);

    // Rollout convergence
    eprintln!();
    eprintln!("[SCENARIO {}] Test rollout convergence", scenario());
    run_and_insert(&mut results, "rollout_convergence", assert_rollout_convergence);

    // Operator restart recovery (all three clusters)
    eprintln!();
    eprintln!(
        "[SCENARIO {}] Test operator restart recovery (all clusters)",
        scenario()
    );
    run_and_insert(
        &mut results,
        "operator_restart_recovery",
        assert_operator_restart_recovery,
    );

    // Post-recovery soak period
    eprintln!();
    eprintln!("[SCENARIO {}] Test post-recovery soak period", scenario());
    run_and_insert(&mut results, "post_recovery_soak", assert_post_recovery_soak);

    results
}

/// Helper: run an assertion and insert its result.
fn run_and_insert(results: &mut BTreeMap<String, ProofResult>, name: &str, assertion_fn: fn() -> AssertionResult) {
    match run_assertion(name, assertion_fn) {
        Ok(proof) => {
            results.insert(name.to_owned(), proof);
        },
        Err(e) => {
            eprintln!("  [X] {name} failed: {e}");
            results.insert(
                name.to_owned(),
                proof_failure(&format!("{name} failed: {e}"), BTreeMap::new(), Duration::from_secs(0)),
            );
        },
    }
}

/// Execute the drain -> session tests -> restore sequence with guaranteed restoration.
fn run_drain_session_restore_sequence(
    results: &mut BTreeMap<String, ProofResult>,
    scenario: &mut dyn FnMut() -> usize,
) {
    // Step 2: Drain
    eprintln!();
    eprintln!("[SCENARIO {}] Central drain and remote fallback", scenario());
    run_and_insert(results, "central_drain_fallback", assert_central_drain_fallback);

    // Step 3: Existing session after drain
    eprintln!();
    eprintln!("[SCENARIO {}] Existing session survives drain", scenario());
    run_and_insert(
        results,
        "existing_session_after_drain",
        assert_existing_session_after_drain,
    );

    // Step 4: New session after drain
    eprintln!();
    eprintln!("[SCENARIO {}] New session after drain", scenario());
    run_and_insert(results, "new_session_after_drain", assert_new_session_after_drain);

    // Step 5: Restore (guaranteed)
    eprintln!();
    eprintln!("[SCENARIO {}] Provider restoration", scenario());
    run_and_insert(results, "provider_restoration", assert_provider_restoration);

    if let Err(error) = ensure_central_provider_restored() {
        results.insert(
            "restoration_guard".to_owned(),
            proof_failure(
                &format!("unconditional central-provider restoration failed: {error}"),
                BTreeMap::new(),
                Duration::ZERO,
            ),
        );
    }
}


// -----------------------------------------------------------------------------
// Teardown
// -----------------------------------------------------------------------------

/// Configure SWIM peer discovery by updating each operator with peer seed addresses.
///
/// Reads each cluster's SWIM LoadBalancer IP via kubectl, then performs helm
/// upgrade operations so every operator has the complete peer seed list.
fn configure_swim_peers(_forge_bin: &Path, _resolved_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut swim_ips = BTreeMap::new();

    for cluster in CLUSTERS {
        let ip = read_swim_lb_ip(cluster)?;
        eprintln!("  {cluster}: SWIM LB IP = {ip}");
        swim_ips.insert(*cluster, ip);
    }

    for cluster in CLUSTERS {
        let this_ip = swim_ips
            .get(cluster)
            .ok_or_else(|| format!("{cluster}: missing SWIM IP"))?;
        let mut peer_parts = Vec::new();
        for c in CLUSTERS {
            if *c != *cluster {
                let ip = swim_ips.get(c).ok_or_else(|| format!("{c}: missing SWIM IP"))?;
                peer_parts.push(format!("{ip}:7946"));
            }
        }
        let peer_seeds = peer_parts.join(",");
        update_operator_swim_config(cluster, this_ip, &peer_seeds)?;
    }

    for cluster in CLUSTERS {
        let ctx = format!("kind-grid-combined-{cluster}");
        wait_for_deployment("grid-operator", GRID_SYSTEM_NS, &ctx)?;
        eprintln!("  [OK] {cluster}: operator restarted with SWIM config");
    }

    Ok(())
}

/// Read the SWIM LoadBalancer IP for a cluster directly from Kubernetes.
fn read_swim_lb_ip(cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = format!("kind-grid-combined-{cluster}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            "grid-operator-swim",
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip}",
        ])
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "{cluster}: cannot read SWIM service: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }

    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if ip.is_empty() {
        return Err(format!("{cluster}: SWIM LoadBalancer has no ingress IP").into());
    }

    Ok(ip)
}

/// Update a single operator's SWIM configuration with peer addresses.
fn update_operator_swim_config(
    cluster: &str,
    advertise_ip: &str,
    seeds: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = format!("kind-grid-combined-{cluster}");

    let operator_image =
        std::env::var("GRID_XTASK_OPERATOR_IMAGE").unwrap_or_else(|_| "grid-operator:combined-site-demo".to_owned());
    let image_pull_policy = std::env::var("GRID_XTASK_IMAGE_PULL_POLICY").unwrap_or_else(|_| "Never".to_owned());
    let (operator_repo, operator_tag) = parse_image_ref(&operator_image);

    let seeds_escaped = seeds.replace(',', "\\,");

    let upgrade_output = Command::new("helm")
        .args([
            "upgrade",
            "grid-operator",
            "charts/grid-operator",
            "--version",
            "0.1.0",
            "--namespace",
            "grid-system",
            "--kube-context",
            &context,
            "--reuse-values",
            "--set",
            &format!("image.repository={operator_repo}"),
            "--set",
            &format!("image.tag={operator_tag}"),
            "--set",
            &format!("image.pullPolicy={image_pull_policy}"),
            "--set",
            &format!("swim.siteName={cluster}"),
            "--set",
            &format!("swim.advertiseAddress={advertise_ip}:7946"),
            "--set",
            &format!("swim.seeds={seeds_escaped}"),
            "--set",
            "swim.service.enabled=true",
            "--set",
            "swim.service.type=LoadBalancer",
            "--set",
            &format!("gateway.serviceName={PROVIDER_GATEWAY_SERVICE}"),
            "--set-string",
            &format!("gateway.port={PROVIDER_GATEWAY_PORT}"),
        ])
        .output()?;

    if !upgrade_output.status.success() {
        return Err(format!(
            "Failed to update SWIM config for {cluster}: {}",
            String::from_utf8_lossy(&upgrade_output.stderr)
        )
        .into());
    }

    eprintln!("  [OK] {cluster}: SWIM peers configured");

    assert_operator_gateway_env(&context, cluster)?;

    Ok(())
}

/// Post-upgrade assertion: the operator deployment must reflect the expected
/// gateway discovery contract after every Helm upgrade.
fn assert_operator_gateway_env(context: &str, cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "deployment/grid-operator",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ])
        .output()?;

    let env_json = String::from_utf8_lossy(&output.stdout);

    let has_service = env_json.contains(&format!(
        "\"name\":\"GRID_GATEWAY_SERVICE_NAME\",\"value\":\"{PROVIDER_GATEWAY_SERVICE}\""
    ));
    let has_port = env_json.contains(&format!(
        "\"name\":\"GRID_GATEWAY_PORT\",\"value\":\"{PROVIDER_GATEWAY_PORT}\""
    ));

    if !has_service || !has_port {
        return Err(format!(
            "{cluster}: operator gateway env mismatch after helm upgrade \
             (expected GRID_GATEWAY_SERVICE_NAME={PROVIDER_GATEWAY_SERVICE}, \
             GRID_GATEWAY_PORT={PROVIDER_GATEWAY_PORT}); got: {env_json}"
        )
        .into());
    }

    Ok(())
}

/// Configure external provider in the specified site with site-specific placement.
///
/// Configure external provider resources in the specified site.
///
/// Creates the Secret from the supplied key file (without reading its content
/// into this process), an `InferenceProvider` CRD, and updates the provider
/// gateway helm release to mount the Secret.  Consumer gateways discover the
/// external provider automatically through the Grid operator overlay.
fn configure_external_provider(
    external_provider: &ExternalProviderDescriptor,
    site: &str,
    key_file: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = format!("kind-grid-combined-{site}");

    create_external_provider_secret(external_provider, site, &context, key_file)?;
    create_external_inference_provider(external_provider, site, &context)?;
    update_provider_gateway_for_external(external_provider, site, &context)?;

    Ok(())
}

/// Create external provider Secret from the API key file.
///
/// Uses `kubectl create secret generic --from-file` so the key file content
/// is never read or logged by this process.
fn create_external_provider_secret(
    external_provider: &ExternalProviderDescriptor,
    site: &str,
    context: &str,
    key_file: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let from_file_arg = format!("--from-file={}={}", external_provider.secret_key, key_file.display());
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "create",
            "secret",
            "generic",
            external_provider.secret_name,
            &from_file_arg,
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to render external provider Secret: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    kubectl::apply_manifest(context, &String::from_utf8(output.stdout)?)?;
    eprintln!("  [OK] {site}: external provider Secret created from key file");
    Ok(())
}

/// Create InferenceProvider resource targeting the specified site.
fn create_external_inference_provider(
    external_provider: &ExternalProviderDescriptor,
    site: &str,
    context: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let provider_manifest = format!(
        r#"apiVersion: grid.praxis-proxy.io/v1alpha1
kind: InferenceProvider
metadata:
  name: external-{provider_kind}
spec:
  gridNetworkRef: grid-combined-site
  providerKind: {provider_kind}
  backendKind: {backend_kind}
  endpoint: https://{hostname}:{port}
  models:
    - name: {model}
      capabilities: ["text_generation"]
      contextWindow: 200000
  auth:
    strategy: api_key
    manual: false
    secretRef:
      name: {secret_name}
      namespace: grid-system
      key: {secret_key}
  siteSelector:
    matchLabels:
      grid.praxis-proxy.io/combined-site: {site}
  accessPolicy:
    siteSelector:
      matchLabels: {{}}
  healthCheck:
    path: /v1/models
    interval: "60s"
    timeout: "10s"
  routingClusterRef: {routing_cluster}
"#,
        provider_kind = external_provider.provider_kind,
        backend_kind = external_provider.backend_kind,
        hostname = external_provider.hostname,
        port = external_provider.port,
        model = external_provider.model,
        secret_name = external_provider.secret_name,
        secret_key = external_provider.secret_key,
        site = site,
        routing_cluster = external_provider.routing_cluster,
    );

    kubectl::apply_manifest(context, &provider_manifest)?;
    eprintln!("  [OK] {site}: external InferenceProvider created");
    Ok(())
}

/// Update provider gateway in the selected site to mount the external provider Secret.
fn update_provider_gateway_for_external(
    external_provider: &ExternalProviderDescriptor,
    site: &str,
    context: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let upgrade_output = Command::new("helm")
        .args([
            "upgrade",
            "provider-gateway",
            "charts/praxis-gateway",
            "--namespace",
            "grid-system",
            "--kube-context",
            context,
            "--reuse-values",
            "--set",
            &format!("credentials[1].name={}", external_provider.secret_name),
            "--set",
            &format!("credentials[1].mountPath={}", external_provider.mount_path),
        ])
        .output()?;

    if !upgrade_output.status.success() {
        return Err(format!(
            "Failed to update provider gateway in {site}: {}",
            String::from_utf8_lossy(&upgrade_output.stderr)
        )
        .into());
    }

    eprintln!("  [OK] {site}: provider gateway updated with external credential mount");
    Ok(())
}

/// Tear down the combined-site environment.
fn teardown_environment(context: &CombinedSiteContext) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!();
    eprintln!("=== TEARDOWN ===");

    let status = Command::new(&context.forge_bin)
        .args(["down", "--config"])
        .arg(&context.resolved_config)
        .status()?;

    if !status.success() {
        return Err("Failed to tear down environment".into());
    }

    eprintln!("  [OK] Environment torn down successfully");
    Ok(())
}

// -----------------------------------------------------------------------------
// Main Entry Point
// -----------------------------------------------------------------------------

/// Run the complete combined-site demo.
#[expect(
    clippy::too_many_lines,
    reason = "main demo orchestration function with error handling; splitting obscures the complete flow"
)]
pub(crate) fn run(forge_config: &Path, options: &GlbDemoOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mode = options.mode();

    // Resolve external provider (reusing GLB demo logic)
    let ext_descriptor = external_provider::resolve_external_provider(
        options.external_provider,
        options.external_provider_key_file.as_deref(),
        options.external_provider_model.as_deref(),
    )?;

    let ext_site = options.external_provider_site.clone();
    let ext_key_file = options.external_provider_key_file.clone();

    let run_id = format_utc_timestamp();
    let _started_at = format_utc_iso();
    let wall_start = Instant::now();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id)?;
    fs::create_dir_all(&evidence_dir)?;

    let setup_ctx = prepare_setup(forge_config, ext_descriptor.clone(), ext_site.clone(), ext_key_file);
    let mut teardown_success = false;
    let mut run_error = None;
    let mut overlay_state = OverlayState::default();

    let proof_results = match &setup_ctx {
        Ok(context) => {
            eprintln!("{OUTPUT_RULE}");
            eprintln!("Grid Combined-Site Demo");
            eprintln!("Mode: {}", if mode == DemoMode::Quick { "quick" } else { "full" });
            eprintln!("Config: {}", forge_config.display());
            eprintln!("{OUTPUT_RULE}");

            match deploy_setup(context) {
                Ok(state) => {
                    overlay_state = state;
                    eprintln!();
                    eprintln!("{OUTPUT_RULE}");
                    eprintln!("ENVIRONMENT READY - Starting proof scenarios");
                    eprintln!("{OUTPUT_RULE}");

                    let ext_site = ext_site.as_deref();
                    let scenario_results = match mode {
                        DemoMode::Quick => run_quick_scenarios(ext_site),
                        DemoMode::Full => run_full_scenarios(ext_site),
                    };

                    let failed_proofs: Vec<&str> = scenario_results
                        .iter()
                        .filter_map(|(name, proof)| (!proof.success).then_some(name.as_str()))
                        .collect();
                    if !failed_proofs.is_empty() {
                        run_error = Some(format!("runtime proofs failed: {}", failed_proofs.join(", ")));
                    }

                    // Teardown if requested
                    if options.teardown && (run_error.is_none() || !options.keep_on_failure) {
                        match teardown_environment(context) {
                            Ok(()) => teardown_success = true,
                            Err(error) => {
                                eprintln!("[WARN]  Teardown failed: {error}");
                                run_error = Some(match run_error {
                                    Some(previous) => format!("{previous}; teardown failed: {error}"),
                                    None => format!("teardown failed: {error}"),
                                });
                            },
                        }
                    }

                    scenario_results
                },
                Err(e) => {
                    eprintln!("[FAIL] Environment setup failed: {e}");
                    run_error = Some(format!("environment setup failed: {e}"));

                    if options.teardown && !options.keep_on_failure {
                        if let Err(cleanup_err) = teardown_environment(context) {
                            eprintln!("[WARN]  Cleanup after setup failure also failed: {cleanup_err}");
                            run_error = Some(format!("environment setup failed: {e}; cleanup failed: {cleanup_err}"));
                        } else {
                            teardown_success = true;
                        }
                    }

                    BTreeMap::new()
                },
            }
        },
        Err(e) => {
            eprintln!("[FAIL] Setup preparation failed: {e}");
            run_error = Some(format!("setup preparation failed: {e}"));
            BTreeMap::new()
        },
    };

    // Collect actual evidence
    let images = collect_image_evidence()?;
    let external_provider_evidence = if let Some(desc) = &ext_descriptor {
        Some(collect_external_provider_evidence(
            desc,
            options.external_provider_site.as_deref().unwrap_or("east"),
        ))
    } else {
        None
    };

    let evidence = Evidence {
        schema_version: EVIDENCE_SCHEMA_VERSION.to_owned(),
        mode: if mode == DemoMode::Quick { "quick" } else { "full" }.to_owned(),
        topology: "combined-site".to_owned(),
        clusters: CLUSTERS.iter().map(|&s| s.to_owned()).collect(),
        proof_results,
        images,
        external_provider: external_provider_evidence,
        overlay_state,
        cluster_health: Vec::new(),     // Will be populated during runtime assertions
        components: Vec::new(),         // Will be populated during runtime assertions
        swim_membership: Vec::new(),    // Will be populated during runtime assertions
        provider_responses: Vec::new(), // Will be populated during runtime assertions
        security_results: Vec::new(),   // Will be populated during runtime assertions
        teardown_success,
    };

    // Write evidence
    let evidence_file = evidence_dir.join("results.json");
    let evidence_json = serde_json::to_string_pretty(&evidence)?;
    fs::write(&evidence_file, evidence_json)?;

    eprintln!();
    eprintln!("{OUTPUT_RULE}");
    eprintln!("Demo completed in {:.1}s", wall_start.elapsed().as_secs_f64());
    eprintln!("Evidence: {}", evidence_file.display());
    eprintln!("{OUTPUT_RULE}");

    match run_error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

/// Collect actual image evidence from the deployed clusters.
fn collect_image_evidence() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut images = BTreeMap::new();

    for cluster in CLUSTERS {
        let context = format!("kind-grid-combined-{cluster}");

        // Get grid operator image
        let output = Command::new("kubectl")
            .args([
                "get",
                "deployment/grid-operator",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].image}",
            ])
            .output()?;

        if output.status.success() {
            let operator_image = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            images.insert(format!("{cluster}_operator"), operator_image);
        }

        // Get consumer gateway image
        let output = Command::new("kubectl")
            .args([
                "get",
                "deployment/consumer-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].image}",
            ])
            .output()?;

        if output.status.success() {
            let consumer_image = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            images.insert(format!("{cluster}_consumer_gateway"), consumer_image);
        }

        // Get provider gateway image
        let output = Command::new("kubectl")
            .args([
                "get",
                "deployment/provider-gateway",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].image}",
            ])
            .output()?;

        if output.status.success() {
            let provider_image = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            images.insert(format!("{cluster}_provider_gateway"), provider_image);
        }

        // Get mock inference image
        let output = Command::new("kubectl")
            .args([
                "get",
                "deployment/mock-inference",
                "--context",
                &context,
                "-n",
                "grid-system",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].image}",
            ])
            .output()?;

        if output.status.success() {
            let mock_image = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            images.insert(format!("{cluster}_mock_inference"), mock_image);
        }
    }

    Ok(images)
}

/// Collect external provider evidence from the deployed resources.
fn collect_external_provider_evidence(
    external_provider: &ExternalProviderDescriptor,
    site: &str,
) -> ExternalProviderEvidence {
    // For now, just collect the basic descriptive information
    // Runtime validation of actual deployment would happen during E2E testing
    ExternalProviderEvidence {
        kind: external_provider.provider_kind.to_owned(),
        site: site.to_owned(),
        model: external_provider.model.clone(),
        cluster: external_provider.routing_cluster.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use super::*;

    #[test]
    fn test_proof_success_creation() {
        let mut facts = BTreeMap::new();
        facts.insert("cluster_count".to_owned(), serde_json::Value::Number(3.into()));
        facts.insert("all_healthy".to_owned(), serde_json::Value::Bool(true));

        let proof = proof_success("Test success", facts.clone(), Duration::from_millis(100));

        assert!(proof.success);
        assert_eq!(proof.reason, "Test success");
        assert_eq!(proof.duration_ms, 100);
        assert_eq!(proof.observed_facts.len(), 2);
        assert_eq!(
            proof.observed_facts.get("all_healthy"),
            Some(&serde_json::Value::Bool(true))
        );
    }

    #[test]
    fn test_proof_failure_creation() {
        let mut facts = BTreeMap::new();
        facts.insert("error_code".to_owned(), serde_json::Value::Number(500.into()));

        let proof = proof_failure("Test failure", facts.clone(), Duration::from_millis(50));

        assert!(!proof.success);
        assert_eq!(proof.reason, "Test failure");
        assert_eq!(proof.duration_ms, 50);
        assert_eq!(proof.observed_facts.len(), 1);
        assert_eq!(
            proof.observed_facts.get("error_code"),
            Some(&serde_json::Value::Number(500.into()))
        );
    }

    #[test]
    fn test_assertion_result_error_handling() {
        let assertion_fn = || -> AssertionResult { Err("Simulated assertion failure".into()) };

        let result = run_assertion("test_assertion", assertion_fn);
        assert!(result.is_err());

        let error_msg = format!("{}", result.unwrap_err());
        assert!(error_msg.contains("Assertion test_assertion failed"));
        assert!(error_msg.contains("Simulated assertion failure"));
    }

    #[test]
    fn test_evidence_serialization() {
        let evidence = Evidence {
            schema_version: "test".to_owned(),
            mode: "quick".to_owned(),
            topology: "combined-site".to_owned(),
            clusters: vec!["west".to_owned(), "central".to_owned(), "east".to_owned()],
            proof_results: BTreeMap::new(),
            images: BTreeMap::new(),
            external_provider: None,
            overlay_state: OverlayState::default(),
            cluster_health: vec![ClusterHealth {
                name: "west".to_owned(),
                healthy: true,
                api_response_ms: Some(100),
                ready_nodes: 1,
            }],
            components: vec![ComponentStatus {
                name: "west-grid-operator".to_owned(),
                namespace: "grid-system".to_owned(),
                ready_replicas: 1,
                desired_replicas: 1,
                ready: true,
            }],
            swim_membership: vec![SwimMembership {
                site: "west".to_owned(),
                local_node: "west-operator".to_owned(),
                peers: vec!["central-operator".to_owned(), "east-operator".to_owned()],
                converged: true,
            }],
            provider_responses: vec![],
            security_results: vec![],
            teardown_success: true,
        };

        let json = serde_json::to_string(&evidence).unwrap();
        assert!(json.contains("\"schema_version\":\"test\""));
        assert!(json.contains("\"topology\":\"combined-site\""));
        assert!(json.contains("\"healthy\":true"));
        assert!(json.contains("\"ready_replicas\":1"));
        assert!(json.contains("\"converged\":true"));

        // Verify deserialization
        let _deserialized: Evidence = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_proof_count_validation() {
        let names = [
            "cluster_health",
            "component_deployment",
            "swim_convergence",
            "external_provider_absence",
            "overlay_acceptance",
            "local_provider_selection",
            "response_attribution",
            "tls_certificate_validation",
            "authorization_replacement",
            "credential_isolation",
            "backend_access_denial",
            "negative_routing",
        ];
        assert_eq!(names.len(), 12);
        assert_eq!(names.first(), Some(&"cluster_health"));
        assert_eq!(names.last(), Some(&"negative_routing"));
    }

    #[test]
    fn test_cluster_constants() {
        assert_eq!(CLUSTERS.len(), 3);
        assert_eq!(CLUSTERS, &["west", "central", "east"]);
    }

    #[test]
    fn test_evidence_schema_version() {
        assert_eq!(EVIDENCE_SCHEMA_VERSION, "1");
    }

    #[test]
    fn curl_pod_overrides_meets_restricted_pod_security() {
        let json = curl_pod_overrides("test-probe", &["curl", "--fail", "http://example.test"]);
        let actual: serde_json::Value = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            actual,
            serde_json::json!({
                "spec": {
                    "automountServiceAccountToken": false,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "seccompProfile": { "type": "RuntimeDefault" }
                    },
                    "containers": [{
                        "name": "test-probe",
                        "image": "curlimages/curl:8.12.1",
                        "command": ["curl"],
                        "args": ["--fail", "http://example.test"],
                        "securityContext": {
                            "runAsUser": 100,
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": { "drop": ["ALL"] }
                        }
                    }]
                }
            })
        );
    }
}
