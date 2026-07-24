//! GLB ingress failover verifier.
//!
//! Runs prerequisite checks then a structured 13-step verification
//! representing the full GLB failover proof.  Steps 1-6 validate
//! prerequisite infrastructure.  Step 7 (edge identity) is `BLOCKED`
//! when any service uses a placeholder image.  Steps 8-13 cover the
//! failover sequence and are `BLOCKED` until the failover harness is
//! implemented.  The command exits non-zero whenever any step is
//! `FAIL` or `BLOCKED`.

use std::{path::Path, process::Command};

use crate::env::{StepResult, StepStatus, print_validate_all_table, safe_truncate_str, verify};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Edge service name in the GLB demo forge.yaml.
const EDGE_SERVICE: &str = "grid-edge-us-east";

/// Kubernetes namespace for Grid resources.
const GRID_SYSTEM_NS: &str = "grid-system";

/// Cluster name prefix from the GLB demo config.
const CLUSTER_PREFIX: &str = "grid-glb";

/// Expected cluster names in the GLB demo environment.
const CLUSTER_NAMES: &[&str] = &["edge-control", "provider-east", "provider-west"];

/// Required CLI tools (checked during prerequisites).
const REQUIRED_TOOLS: &[&str] = &["kind", "kubectl", "curl", "docker"];

/// Total number of verification steps.
const TOTAL_STEPS: u32 = 13;

/// Reason for steps that depend on the failover harness.
const FAILOVER_NOT_IMPLEMENTED: &str = "failover proof not yet implemented; \
     see environments/grid-glb-demo/README.md";

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Verify GLB ingress failover readiness.
///
/// Checks prerequisites (config, tools, forge binary, placeholder
/// images), then runs a 13-step structured verification.  Exits
/// non-zero if any step is `FAIL` or `BLOCKED`.
///
/// # Errors
///
/// Returns an error if hard prerequisites fail (config, tools,
/// forge binary) or any verification step is not `PASS`.
pub(crate) fn verify_glb_ingress(forge_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("glb-ingress: checking prerequisites...");
    let ctx = check_prerequisites(forge_config)?;

    let mut results: Vec<StepResult> = Vec::new();
    run_steps(&ctx, &mut results);

    eprintln!();
    eprintln!("## GLB Ingress Failover Proof");
    print_validate_all_table(&results);

    let any_not_pass = results.iter().any(|r| r.status != StepStatus::Pass);
    if any_not_pass {
        let fail_count = results.iter().filter(|r| r.status.is_failure()).count();
        let blocked_count = results.iter().filter(|r| r.status == StepStatus::Blocked).count();
        Err(format!(
            "glb-ingress: {fail_count} FAIL, {blocked_count} BLOCKED \
             — failover proof incomplete"
        )
        .into())
    } else {
        eprintln!("glb-ingress: all proof points PASS");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Prerequisites
// ---------------------------------------------------------------------------

/// Validated prerequisite context.
#[derive(Debug)]
struct PrereqContext {
    /// Path to the forge config file.
    config: std::path::PathBuf,
    /// Resolved forge binary path.
    forge_bin: String,
    /// Edge host port parsed from config (used by failover steps).
    _edge_port: u16,
    /// Services blocked by placeholder images.
    placeholders: Vec<(String, String)>,
}

/// Check all prerequisites and return a context for the verification
/// steps.  Fails with a combined error if config, tools, or the forge
/// binary are missing.  Placeholder images are stored in the context
/// for per-step gating (warning, not fatal).
fn check_prerequisites(forge_config: &Path) -> Result<PrereqContext, Box<dyn std::error::Error>> {
    let (errors, forge_bin) = collect_prereq_errors(forge_config);
    if !errors.is_empty() {
        report_prereq_errors(&errors);
        return Err(format!("{} prerequisite(s) failed", errors.len()).into());
    }

    let config_text = std::fs::read_to_string(forge_config)?;
    let placeholders = detect_placeholder_images(&config_text);
    if !placeholders.is_empty() {
        warn_placeholder_images(&placeholders);
    }

    let forge_bin = forge_bin.unwrap_or_else(|| std::process::abort());
    let edge_port = parse_edge_host_port(&config_text, EDGE_SERVICE).unwrap_or(8080);

    Ok(PrereqContext {
        config: forge_config.to_path_buf(),
        forge_bin,
        _edge_port: edge_port,
        placeholders,
    })
}

/// Print prerequisite errors to stderr.
fn report_prereq_errors(errors: &[String]) {
    eprintln!();
    for e in errors {
        eprintln!("  PREREQ FAIL: {e}");
    }
    eprintln!();
}

/// Warn about placeholder images (steps 7+ will be BLOCKED).
fn warn_placeholder_images(placeholders: &[(String, String)]) {
    eprintln!();
    for (svc, img) in placeholders {
        eprintln!("  WARNING: service '{svc}' uses placeholder image '{img}' — steps 7+ will be BLOCKED");
    }
    eprintln!();
}

/// Collect prerequisite errors for config, tools, and forge binary.
fn collect_prereq_errors(forge_config: &Path) -> (Vec<String>, Option<String>) {
    let mut errors: Vec<String> = Vec::new();
    if !forge_config.exists() {
        errors.push(format!("config file not found: {}", forge_config.display()));
    }
    for tool in REQUIRED_TOOLS {
        if !tool_available(tool) {
            errors.push(format!("required tool not found on PATH: {tool}"));
        }
    }
    let forge_bin = resolve_forge_binary();
    if forge_bin.is_none() {
        errors.push(
            "praxis-forge binary not found on PATH or at \
             target/debug/praxis-forge"
                .to_owned(),
        );
    }
    (errors, forge_bin)
}

/// Check whether a CLI tool is available on `PATH` via `which`.
fn tool_available(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Resolve the forge binary: prefer `praxis-forge` on PATH, fall
/// back to `target/debug/praxis-forge`.
fn resolve_forge_binary() -> Option<String> {
    if tool_available("praxis-forge") {
        return Some("praxis-forge".to_owned());
    }
    let local = "target/debug/praxis-forge";
    if Path::new(local).exists() {
        return Some(local.to_owned());
    }
    None
}

/// Detect placeholder images in forge config text.
///
/// Scans for lines containing both `image:` and `PLACEHOLDER`,
/// tracking the nearest preceding `- name:` line as the service name.
pub(crate) fn detect_placeholder_images(config_text: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let mut current_service = String::new();

    for line in config_text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("- name:") {
            rest.trim().clone_into(&mut current_service);
        }
        if trimmed.contains("image:") && trimmed.contains("PLACEHOLDER") {
            let image = trimmed
                .split("image:")
                .nth(1)
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .to_owned();
            results.push((current_service.clone(), image));
        }
    }
    results
}

/// Parse the host port for a named service from forge config text.
///
/// Looks for the `- name: <service>` block and extracts the first
/// `host:` value from its `ports:` section.
pub(crate) fn parse_edge_host_port(config_text: &str, service_name: &str) -> Option<u16> {
    let name_marker = format!("- name: {service_name}");
    let mut in_service = false;

    for line in config_text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("- name:") {
            in_service = trimmed.contains(&name_marker);
            continue;
        }
        if in_service && let Some(rest) = trimmed.strip_prefix("host:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Verification steps
// ---------------------------------------------------------------------------

/// Run all 13 verification steps.
#[expect(
    clippy::too_many_lines,
    reason = "sequential proof steps: each step depends on the previous; splitting obscures the proof flow"
)]
fn run_steps(ctx: &PrereqContext, results: &mut Vec<StepResult>) {
    // Step 1: Forge config validation.
    step_banner(1, "validating forge config");
    let config_ok = record_step("prerequisites", results, || {
        validate_forge_config(&ctx.forge_bin, &ctx.config)
    });
    if !config_ok {
        block_remaining(2, "config validation failed", results);
        return;
    }

    // Step 2: Environment status.
    step_banner(2, "checking environment status");
    let status_json = match run_forge_status(&ctx.forge_bin, &ctx.config) {
        Ok(json) => {
            results.push(StepResult::pass("forge status", "forge status returned OK"));
            json
        },
        Err(e) => {
            results.push(StepResult::fail("forge status", e.as_ref()));
            block_remaining(3, "status unavailable", results);
            return;
        },
    };

    // Step 3: All clusters live.
    step_banner(3, "checking clusters live");
    let clusters_ok = record_step("clusters live", results, || check_clusters_live(&status_json));
    if !clusters_ok {
        block_remaining(4, "clusters not live", results);
        return;
    }

    // Step 4: Provider gateway IPs.
    step_banner(4, "checking provider gateway IPs");
    let gateways_ok = record_step("provider gateway IPs", results, check_provider_gateways_captured);

    // Step 5: Provider gateways reachable.
    step_banner(5, "checking provider gateway reachability");
    if gateways_ok {
        record_step("provider gateways reachable", results, || {
            check_provider_gateways_reachable()
        });
    } else {
        results.push(StepResult::blocked(
            "provider gateways reachable",
            "gateway IPs not captured",
        ));
    }

    // Step 6: Edge config applied.
    step_banner(6, "checking edge config applied");
    record_step("edge config applied", results, check_edge_control_stacks);

    // Step 7: Edge identity captured (blocked if placeholder images).
    step_banner(7, "capturing edge identity");
    if ctx.placeholders.is_empty() {
        record_step("edge identity captured", results, || {
            check_service_identity(&status_json, EDGE_SERVICE)
        });
    } else {
        let reason = format!(
            "placeholder images: {}",
            ctx.placeholders
                .iter()
                .map(|(svc, _)| svc.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        results.push(StepResult::blocked("edge identity captured", reason));
    }

    // Steps 8-13: Failover sequence (not yet implemented).
    for (step, label) in [
        (8, "initial request → provider-east"),
        (9, "provider-east failure injected"),
        (10, "overlay revision changed"),
        (11, "edge reload observed"),
        (12, "second request → provider-west"),
        (13, "edge identity unchanged"),
    ] {
        step_banner(step, label);
        results.push(StepResult::blocked(label, FAILOVER_NOT_IMPLEMENTED));
    }
}

/// Print a step progress banner.
fn step_banner(step: u32, description: &str) {
    eprintln!("glb-ingress: [{step}/{TOTAL_STEPS}] {description}...");
}

/// Record a step result, returning whether it passed.
fn record_step(
    label: &'static str,
    results: &mut Vec<StepResult>,
    f: impl FnOnce() -> Result<String, Box<dyn std::error::Error>>,
) -> bool {
    match f() {
        Ok(evidence) => {
            results.push(StepResult::pass(label, evidence));
            true
        },
        Err(e) => {
            results.push(StepResult::fail(label, e.as_ref()));
            false
        },
    }
}

/// Labels for all 13 steps, indexed from 0.
const STEP_LABELS: &[&str] = &[
    "prerequisites",
    "forge status",
    "clusters live",
    "provider gateway IPs",
    "provider gateways reachable",
    "edge config applied",
    "edge identity captured",
    "initial request → provider-east",
    "provider-east failure injected",
    "overlay revision changed",
    "edge reload observed",
    "second request → provider-west",
    "edge identity unchanged",
];

/// Block all steps from `from_step` (1-indexed) onward.
fn block_remaining(from_step: u32, reason: &str, results: &mut Vec<StepResult>) {
    for label in STEP_LABELS.get((from_step.saturating_sub(1) as usize)..).unwrap_or(&[]) {
        results.push(StepResult::blocked(label, reason.to_owned()));
    }
}

// ---------------------------------------------------------------------------
// Step implementations
// ---------------------------------------------------------------------------

/// Step 1: Validate forge config.
fn validate_forge_config(forge_bin: &str, config: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["config", "validate", "--config", &config.display().to_string()])
        .output()?;
    if output.status.success() {
        Ok("config validation passed".to_owned())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("config validation failed: {}", safe_truncate_str(stderr.trim(), 120)).into())
    }
}

/// Step 2: Run `praxis-forge status --output json` and parse.
pub(crate) fn run_forge_status(
    forge_bin: &str,
    config: &Path,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["status", "--config", &config.display().to_string(), "--output", "json"])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("forge status failed: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let stdout = String::from_utf8(output.stdout)?;
    let json: serde_json::Value = serde_json::from_str(&stdout)?;
    Ok(json)
}

/// Step 3: Verify all expected clusters are live.
pub(crate) fn check_clusters_live(status_json: &serde_json::Value) -> Result<String, Box<dyn std::error::Error>> {
    let clusters = status_json
        .get("data")
        .and_then(|d| d.get("clusters"))
        .and_then(serde_json::Value::as_array)
        .ok_or("status JSON missing data.clusters array")?;

    let mut missing = Vec::new();
    for expected in CLUSTER_NAMES {
        let found = clusters.iter().any(|c| {
            c.get("name").and_then(serde_json::Value::as_str) == Some(expected)
                && c.get("live").and_then(serde_json::Value::as_bool) == Some(true)
        });
        if !found {
            missing.push(*expected);
        }
    }

    if missing.is_empty() {
        Ok(format!("all {} clusters live", CLUSTER_NAMES.len()))
    } else {
        Err(format!("clusters not live: {}", missing.join(", ")).into())
    }
}

/// Step 4: Check provider gateway IPs via kubectl.
fn check_provider_gateways_captured() -> Result<String, Box<dyn std::error::Error>> {
    let mut found = Vec::new();
    for cluster in &["provider-east", "provider-west"] {
        let context = kubectl_context(cluster);
        let ip = get_provider_gateway_ip(&context)?;
        found.push(format!("{cluster}={ip}"));
    }
    Ok(found.join(", "))
}

/// Step 5: Check provider gateways are reachable via curl.
fn check_provider_gateways_reachable() -> Result<String, Box<dyn std::error::Error>> {
    let mut verified = Vec::new();
    for cluster in &["provider-east", "provider-west"] {
        let context = kubectl_context(cluster);
        let ip = get_provider_gateway_ip(&context)?;
        let url = format!("http://{ip}:8080/health");
        let resp = verify::curl_get(&url)?;
        if resp.status != 200 {
            return Err(format!("{cluster} gateway returned HTTP {} (expected 200)", resp.status).into());
        }
        verified.push(*cluster);
    }
    Ok(format!("{} gateways healthy", verified.len()))
}

/// Step 6: Check edge-control stacks are applied.
fn check_edge_control_stacks() -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context("edge-control");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "gridnetwork",
            "--no-headers",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "kubectl get gridnetwork failed: {}",
            safe_truncate_str(stderr.trim(), 120)
        )
        .into());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let count = stdout.lines().count();
    if count == 0 {
        return Err("no GridNetwork resources found on edge-control".into());
    }
    Ok(format!("{count} GridNetwork resource(s) found"))
}

/// Step 7: Verify a service identity (phase, health, containerId,
/// restartCount) from forge status JSON.
pub(crate) fn check_service_identity(
    status_json: &serde_json::Value,
    service_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let svc = find_service_in_status(status_json, service_name)?;

    let phase = svc
        .get("phase")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    if phase != "running" {
        return Err(format!("service '{service_name}' phase={phase} (expected running)").into());
    }

    let health = svc
        .get("health")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    if health != "healthy" && health != "unknown" {
        return Err(format!("service '{service_name}' health={health} (expected healthy)").into());
    }

    let container_id = svc.get("containerId").and_then(serde_json::Value::as_str);
    let Some(id) = container_id.filter(|id| !id.is_empty()) else {
        return Err(format!("service '{service_name}' has no containerId (not running)").into());
    };

    let restart_count = svc.get("restartCount").and_then(serde_json::Value::as_u64);
    if let Some(restarts) = restart_count
        && restarts > 0
    {
        return Err(format!("service '{service_name}' restartCount={restarts} (expected 0)").into());
    }

    Ok(format!(
        "containerId={}, phase=running, restartCount=0",
        safe_truncate_str(id, 12)
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the kubectl context for a GLB demo cluster.
fn kubectl_context(cluster_name: &str) -> String {
    format!("kind-{CLUSTER_PREFIX}-{cluster_name}")
}

/// Get the external IP of the provider-gateway service via kubectl.
fn get_provider_gateway_ip(context: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            "provider-gateway",
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("kubectl get svc failed: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if ip.is_empty() {
        return Err(format!("provider-gateway on {context} has no external IP").into());
    }
    Ok(ip)
}

/// Find a service entry in forge status JSON by name.
pub(crate) fn find_service_in_status<'a>(
    status_json: &'a serde_json::Value,
    service_name: &str,
) -> Result<&'a serde_json::Value, Box<dyn std::error::Error>> {
    let services = status_json
        .get("data")
        .and_then(|d| d.get("services"))
        .and_then(serde_json::Value::as_array)
        .ok_or("status JSON missing data.services array")?;

    services
        .iter()
        .find(|s| s.get("name").and_then(serde_json::Value::as_str) == Some(service_name))
        .ok_or_else(|| format!("service '{service_name}' not found in status").into())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Sample forge.yaml with placeholder images.
    fn sample_config_with_placeholders() -> &'static str {
        "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: grid-glb-demo
spec:
  services:
    - name: grid-overlay-sync-us-east
      image: \"ghcr.io/praxis-proxy/grid-overlay-sync:sha-PLACEHOLDER\"
    - name: grid-edge-us-east
      image: \"ghcr.io/praxis-proxy/praxis-ai:sha-PLACEHOLDER\"
      ports:
        - bindAddress: \"127.0.0.1\"
          host: 8080
          container: 8080"
    }

    /// Sample forge.yaml with real SHA tags.
    fn sample_config_no_placeholders() -> &'static str {
        "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: grid-glb-demo
spec:
  services:
    - name: grid-overlay-sync-us-east
      image: \"ghcr.io/praxis-proxy/grid-overlay-sync:sha-abc123\"
    - name: grid-edge-us-east
      image: \"ghcr.io/praxis-proxy/praxis-ai:sha-def456\"
      ports:
        - bindAddress: \"127.0.0.1\"
          host: 9090
          container: 9090"
    }

    /// Build a status JSON with configurable service fields.
    fn status_with_service(
        phase: &str,
        health: &str,
        container_id: Option<&str>,
        restart_count: Option<u32>,
    ) -> serde_json::Value {
        serde_json::json!({
            "status": "ok",
            "data": {
                "clusters": [
                    {"name": "edge-control", "statePhase": "running", "live": true},
                    {"name": "provider-east", "statePhase": "running", "live": true},
                    {"name": "provider-west", "statePhase": "running", "live": true}
                ],
                "services": [{
                    "name": EDGE_SERVICE,
                    "containerName": "grid-glb-grid-edge-us-east",
                    "phase": phase,
                    "health": health,
                    "containerId": container_id,
                    "startedAt": "2026-07-22T14:31:00Z",
                    "restartCount": restart_count,
                }]
            }
        })
    }

    #[test]
    fn detects_placeholder_images() {
        let placeholders = detect_placeholder_images(sample_config_with_placeholders());
        assert_eq!(placeholders.len(), 2, "should find 2 placeholders");
        assert_eq!(
            placeholders.first().map(|(n, _)| n.as_str()),
            Some("grid-overlay-sync-us-east"),
            "first placeholder service name"
        );
        assert_eq!(
            placeholders.get(1).map(|(n, _)| n.as_str()),
            Some("grid-edge-us-east"),
            "second placeholder service name"
        );
    }

    #[test]
    fn no_placeholders_in_clean_config() {
        let placeholders = detect_placeholder_images(sample_config_no_placeholders());
        assert!(placeholders.is_empty(), "should find no placeholders in clean config");
    }

    #[test]
    fn placeholder_images_detected() {
        let placeholders = detect_placeholder_images(sample_config_with_placeholders());
        assert!(!placeholders.is_empty(), "should detect placeholder images");
        assert!(
            placeholders.iter().any(|(_, img)| img.contains("PLACEHOLDER")),
            "should contain PLACEHOLDER tag: {placeholders:?}",
        );
    }

    #[test]
    fn parses_forge_status_clusters() {
        let status = status_with_service("running", "healthy", Some("abc123"), Some(0));
        let result = check_clusters_live(&status);
        assert!(result.is_ok(), "all clusters should be live: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("3"), "should report 3 clusters: {evidence}");
    }

    #[test]
    fn service_identity_running_healthy() {
        let status = status_with_service("running", "healthy", Some("abcdef1234567890"), Some(0));
        let result = check_service_identity(&status, EDGE_SERVICE);
        assert!(result.is_ok(), "should pass: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("containerId="), "evidence: {evidence}");
        assert!(evidence.contains("restartCount=0"), "evidence: {evidence}");
    }

    #[test]
    fn service_identity_phase_stopped_fails() {
        let status = status_with_service("stopped", "unknown", Some("abc123"), Some(0));
        let Err(err) = check_service_identity(&status, EDGE_SERVICE) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("phase=stopped"), "error: {msg}");
    }

    #[test]
    fn service_identity_unhealthy_fails() {
        let status = status_with_service("running", "unhealthy", Some("abc123"), Some(0));
        let Err(err) = check_service_identity(&status, EDGE_SERVICE) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("health=unhealthy"), "error: {msg}");
    }

    #[test]
    fn service_identity_restart_count_nonzero_fails() {
        let status = status_with_service("running", "healthy", Some("abc123"), Some(3));
        let Err(err) = check_service_identity(&status, EDGE_SERVICE) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("restartCount=3"), "error: {msg}");
    }

    #[test]
    fn service_identity_no_container_id_fails() {
        let status = status_with_service("running", "healthy", None, Some(0));
        let Err(err) = check_service_identity(&status, EDGE_SERVICE) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("no containerId"), "error: {msg}");
    }

    #[test]
    fn parses_edge_host_port_from_config() {
        let port = parse_edge_host_port(sample_config_with_placeholders(), "grid-edge-us-east");
        assert_eq!(port, Some(8080), "should parse host port 8080");
    }

    #[test]
    fn parses_custom_edge_host_port() {
        let port = parse_edge_host_port(sample_config_no_placeholders(), "grid-edge-us-east");
        assert_eq!(port, Some(9090), "should parse host port 9090");
    }

    #[test]
    fn edge_port_none_for_unknown_service() {
        let port = parse_edge_host_port(sample_config_with_placeholders(), "nonexistent");
        assert_eq!(port, None, "unknown service should return None");
    }

    #[test]
    fn blocked_steps_cause_nonzero_exit() {
        let results = [
            StepResult::pass("step-a", "ok"),
            StepResult::blocked("step-b", "not implemented"),
        ];
        let any_not_pass = results.iter().any(|r| r.status != StepStatus::Pass);
        assert!(any_not_pass, "BLOCKED step should prevent clean exit");
    }
}
